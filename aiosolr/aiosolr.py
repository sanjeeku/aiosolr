# coding: utf-8
import time
import json
from urllib.parse import urlencode
from xml.etree import ElementTree
import asyncio
import aiohttp
from .log import LOG
from .exceptions import SolrError
from . import utils
from .result_cls import Results
from .error_extractor import extract_error, make_error_msg


class Solr(object):

    def __init__(self, url, decoder=None, timeout=60, results_cls=Results, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self.loop = loop
        self.decoder = decoder or json.JSONDecoder()
        self.url = url
        self.timeout = timeout
        self.log = self._get_log()
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(use_dns_cache=True, loop=loop),
            loop=loop)
        self.results_cls = results_cls

    def _get_log(self):
        return LOG

    def _create_full_url(self, path=''):
        if len(path):
            return '/'.join([self.url.rstrip('/'), path.lstrip('/')])

        # No path? No problem.
        return self.url

    def get_multipart_form_data(self, params, files):
        form_data = aiohttp.helpers.FormData()
        for k, v in params.items():
            form_data.add_field(k, v)
        for file_obj in files:
            form_data.add_field(file_obj.name, file_obj)
        return form_data

    @asyncio.coroutine
    def _send_request(self, method, path='', body=None, headers=None, files=None):
        url = self._create_full_url(path)
        method = method.lower()
        log_body = body

        if headers is None:
            headers = {}

        if log_body is None:
            log_body = ''
        elif not isinstance(log_body, str):
            log_body = repr(body)

        self.log.debug("Starting request to '%s' (%s) with body '%s'...",
                       url, method, log_body[:10])
        start_time = time.time()

        # Everything except the body can be Unicode. The body must be
        # encoded to bytes to work properly on Py3.
        bytes_body = body

        if bytes_body is not None:
            bytes_body = utils.force_bytes(body)

        if files:
            data = self.get_multipart_form_data(bytes_body, files)
        else:
            data = bytes_body

        try:
            with aiohttp.Timeout(self.timeout, loop=self.loop):
                resp = yield from self.session.request(
                    method, url, data=data, headers=headers)
        except aiohttp.errors.ClientTimeoutError as err:
            error_message = "Connection to server '%s' timed out: %s"
            self.log.error(error_message, url, err, exc_info=True)
            raise SolrError(error_message % (url, err))
        except aiohttp.errors.ClientConnectionError as err:
            error_message = "Failed to connect to server at '%s', are you sure that URL is correct? Checking it in a browser might help: %s"
            params = (url, err)
            self.log.error(error_message, *params, exc_info=True)
            raise SolrError(error_message % params)
        except aiohttp.errors.ClientError as err:
            error_message = "Unhandled error: %s %s: %s"
            self.log.error(error_message, method, url, err, exc_info=True)
            raise SolrError(error_message % (method, url, err))

        end_time = time.time()
        self.log.info("Finished '%s' (%s) with body '%s' in %0.3f seconds.",
                      url, method, log_body[:10], end_time - start_time)

        if int(resp.status) != 200:
            error_message = "Solr responded with an error (HTTP %s): %s"
            reason = resp.headers.get('reason', None)
            full_response = None
            if reason is None:
                reason, full_response = yield from extract_error(resp)
            solr_message = make_error_msg(reason, full_response)
            self.log.error(error_message, resp.status, solr_message,
                           extra={'data': {'headers': resp.headers,
                                           'response': resp.content}})
            raise SolrError(error_message % (resp.status, solr_message))

        content = yield from resp.text()
        return utils.force_unicode(content)

    async def _select(self, params, search_handler='select'):
        # specify json encoding of results
        params['wt'] = 'json'
        params_encoded = urlencode(params, doseq=True)

        if len(params_encoded) < 1024:
            # Typical case.
            path = '%s/?%s' % (search_handler, params_encoded)
            response = await self._send_request('get', path)
            return response
        else:
            # Handles very long queries by submitting as a POST.
            path = '%s/' % search_handler
            headers = {
                'Content-type': 'application/x-www-form-urlencoded; charset=utf-8',
            }
            response = await self._send_request(
                'post', path, body=params_encoded, headers=headers)
            return response

    def _is_null_value(self, value):
        return utils.is_null_value(value)

    def _from_python(self, value):
        return utils.from_python(value)

    def _to_python(self, value):
        return utils.to_python(value)

    def _build_doc(self, doc, boost=None, fieldUpdates=None):
        doc_elem = ElementTree.Element('doc')

        for key, value in doc.items():
            if key == 'boost':
                doc_elem.set('boost', utils.force_unicode(value))
                continue

            # To avoid multiple code-paths we'd like to treat all of our values as iterables:
            if isinstance(value, (list, tuple)):
                values = value
            else:
                values = (value, )

            for bit in values:
                if self._is_null_value(bit):
                    continue

                attrs = {'name': key}

                if fieldUpdates and key in fieldUpdates:
                    attrs['update'] = fieldUpdates[key]

                if boost and key in boost:
                    attrs['boost'] = utils.force_unicode(boost[key])

                field = ElementTree.Element('field', **attrs)
                field.text = self._from_python(bit)

                doc_elem.append(field)

        return doc_elem

    async def _update(self, message, clean_ctrl_chars=True, commit=True, softCommit=False, waitFlush=None, waitSearcher=None, overwrite=None):
        """
        Posts the given xml message to http://<self.url>/update and
        returns the result.

        Passing `clean_ctrl_chars` as False will prevent the message from being cleaned
        of control characters (default True). This is done by default because
        these characters would cause Solr to fail to parse the XML. Only pass
        False if you're positive your data is clean.
        """
        path = 'update/'

        # Per http://wiki.apache.org/solr/UpdateXmlMessages, we can append a
        # ``commit=true`` to the URL and have the commit happen without a
        # second request.
        query_vars = []

        if commit is not None:
            query_vars.append('commit=%s' % str(bool(commit)).lower())
        elif softCommit is not None:
            query_vars.append('softCommit=%s' % str(bool(softCommit)).lower())

        if waitFlush is not None:
            query_vars.append('waitFlush=%s' % str(bool(waitFlush)).lower())

        if overwrite is not None:
            query_vars.append('overwrite=%s' % str(bool(overwrite)).lower())

        if waitSearcher is not None:
            query_vars.append('waitSearcher=%s' % str(bool(waitSearcher)).lower())

        if query_vars:
            path = '%s?%s' % (path, '&'.join(query_vars))

        # Clean the message of ctrl characters.
        if clean_ctrl_chars:
            message = utils.sanitize(message)

        response = await self._send_request('post', path, message, {'Content-type': 'text/xml; charset=utf-8'})
        return response

    async def _suggest_terms(self, params):
        # specify json encoding of results
        params['wt'] = 'json'
        path = 'terms/?%s' % urlencode(params, doseq=True)
        response = await self._send_request('get', path)
        return response

    async def _mlt(self, params):
        # specify json encoding of results
        params['wt'] = 'json'
        path = 'mlt/?%s' % urlencode(params, doseq=True)
        response = await self._send_request('get', path)
        return response

    async def search(self, q, search_handler='select', **kwargs):
        """
        Performs a search and returns the results.

        Requires a ``q`` for a string version of the query to run.

        Optionally accepts ``**kwargs`` for additional options to be passed
        through the Solr URL.

        Returns ``self.results_cls`` class object (defaults to
        ``pysolr.Results``)

        Usage::

            # All docs.
            results = yield from solr.search('*:*')

            # Search with highlighting.
            results = yield from solr.search('ponies', **{
                'hl': 'true',
                'hl.fragsize': 10,
            })

        """
        params = {'q': q}
        params.update(kwargs)
        response = await self._select(params, search_handler)
        decoded = self.decoder.decode(response)

        self.log.debug(
            "Found '%s' search results.",
            # cover both cases: there is no response key or value is None
            (decoded.get('response', {}) or {}).get('numFound', 0)
        )
        return self.results_cls(decoded)

    async def more_like_this(self, q, mltfl, **kwargs):
        """
        Finds and returns results similar to the provided query.

        Returns ``self.results_cls`` class object (defaults to
        ``pysolr.Results``)

        Requires Solr 1.3+.

        Usage::

            similar = yield from solr.more_like_this('id:doc_234', 'text')

        """
        params = {
            'q': q,
            'mlt.fl': mltfl,
        }
        params.update(kwargs)
        response = await self._mlt(params)
        decoded = self.decoder.decode(response)

        self.log.debug(
            "Found '%s' MLT results.",
            # cover both cases: there is no response key or value is None
            (decoded.get('response', {}) or {}).get('numFound', 0)
        )
        return self.results_cls(decoded)

    async def suggest_terms(self, fields, prefix, **kwargs):
        """
        Accepts a list of field names and a prefix

        Returns a dictionary keyed on field name containing a list of
        ``(term, count)`` pairs

        Requires Solr 1.4+.
        """
        params = {
            'terms.fl': fields,
            'terms.prefix': prefix,
        }
        params.update(kwargs)
        response = await self._suggest_terms(params)
        result = self.decoder.decode(response)
        terms = result.get("terms", {})
        res = {}

        # in Solr 1.x the value of terms is a flat list:
        #   ["field_name", ["dance",23,"dancers",10,"dancing",8,"dancer",6]]
        #
        # in Solr 3.x the value of terms is a dict:
        #   {"field_name": ["dance",23,"dancers",10,"dancing",8,"dancer",6]}
        if isinstance(terms, (list, tuple)):
            terms = dict(zip(terms[0::2], terms[1::2]))

        for field, values in terms.items():
            tmp = list()

            while values:
                tmp.append((values.pop(0), values.pop(0)))

            res[field] = tmp

        self.log.debug("Found '%d' Term suggestions results.", sum(len(j) for i, j in res.items()))
        return res


    async def add(self, docs, boost=None, fieldUpdates=None, commit=True, softCommit=False, commitWithin=None, waitFlush=None, waitSearcher=None, overwrite=None):
        """
        Adds or updates documents.

        Requires ``docs``, which is a list of dictionaries. Each key is the
        field name and each value is the value to index.

        Optionally accepts ``commit``. Default is ``True``.

        Optionally accepts ``softCommit``. Default is ``False``.

        Optionally accepts ``boost``. Default is ``None``.

        Optionally accepts ``fieldUpdates``. Default is ``None``.

        Optionally accepts ``commitWithin``. Default is ``None``.

        Optionally accepts ``waitFlush``. Default is ``None``.

        Optionally accepts ``waitSearcher``. Default is ``None``.

        Optionally accepts ``overwrite``. Default is ``None``.

        Usage::

            yield from solr.add([
                {
                    "id": "doc_1",
                    "title": "A test document",
                },
                {
                    "id": "doc_2",
                    "title": "The Banana: Tasty or Dangerous?",
                },
            ])
        """
        start_time = time.time()
        self.log.debug("Starting to build add request...")
        message = ElementTree.Element('add')

        if commitWithin:
            message.set('commitWithin', commitWithin)

        for doc in docs:
            message.append(self._build_doc(doc, boost=boost, fieldUpdates=fieldUpdates))

        # This returns a bytestring. Ugh.
        m = ElementTree.tostring(message, encoding='utf-8')
        # Convert back to Unicode please.
        m = utils.force_unicode(m)

        end_time = time.time()
        self.log.debug("Built add request of %s docs in %0.2f seconds.", len(message), end_time - start_time)
        response = await self._update(m, commit=commit, softCommit=softCommit, waitFlush=waitFlush, waitSearcher=waitSearcher, overwrite=overwrite)
        return response


    async def commit(self, softCommit=False, waitFlush=None, waitSearcher=None, expungeDeletes=None):
        """
        Forces Solr to write the index data to disk.

        Optionally accepts ``expungeDeletes``. Default is ``None``.

        Optionally accepts ``waitFlush``. Default is ``None``.

        Optionally accepts ``waitSearcher``. Default is ``None``.

        Optionally accepts ``softCommit``. Default is ``False``.

        Usage::

            yield from solr.commit()

        """
        if expungeDeletes is not None:
            msg = '<commit expungeDeletes="%s" />' % str(bool(expungeDeletes)).lower()
        else:
            msg = '<commit />'

        response = await self._update(
            msg,
            softCommit=softCommit,
            waitFlush=waitFlush,
            waitSearcher=waitSearcher)
        return response


    async def optimize(self, waitFlush=None, waitSearcher=None, maxSegments=None):
        """
        Tells Solr to streamline the number of segments used, essentially a
        defragmentation operation.

        Optionally accepts ``maxSegments``. Default is ``None``.

        Optionally accepts ``waitFlush``. Default is ``None``.

        Optionally accepts ``waitSearcher``. Default is ``None``.

        Usage::

            yield from solr.optimize()

        """
        if maxSegments:
            msg = '<optimize maxSegments="%d" />' % maxSegments
        else:
            msg = '<optimize />'

        response = await self._update(
            msg, waitFlush=waitFlush, waitSearcher=waitSearcher)
        return response


    async def delete(self, id=None, q=None, commit=True, waitFlush=None, waitSearcher=None):
        """
        Deletes documents.

        Requires *either* ``id`` or ``query``. ``id`` is if you know the
        specific document id to remove. ``query`` is a Lucene-style query
        indicating a collection of documents to delete.

        Optionally accepts ``commit``. Default is ``True``.

        Optionally accepts ``waitFlush``. Default is ``None``.

        Optionally accepts ``waitSearcher``. Default is ``None``.

        Usage::

            yield from solr.delete(id='doc_12')
            yield from solr.delete(q='*:*')

        """
        if id is None and q is None:
            raise ValueError('You must specify "id" or "q".')
        elif id is not None and q is not None:
            raise ValueError('You many only specify "id" OR "q", not both.')
        elif id is not None:
            m = '<delete><id>%s</id></delete>' % id
        elif q is not None:
            m = '<delete><query>%s</query></delete>' % q

        response = await self._update(m, commit=commit, waitFlush=waitFlush, waitSearcher=waitSearcher)
        return response


    async def extract(self, file_obj, extractOnly=True, **kwargs):
        """
        POSTs a file to the Solr ExtractingRequestHandler so rich content can
        be processed using Apache Tika. See the Solr wiki for details:

            http://wiki.apache.org/solr/ExtractingRequestHandler

        The ExtractingRequestHandler has a very simple model: it extracts
        contents and metadata from the uploaded file and inserts it directly
        into the index. This is rarely useful as it allows no way to store
        additional data or otherwise customize the record. Instead, by default
        we'll use the extract-only mode to extract the data without indexing it
        so the caller has the opportunity to process it as appropriate; call
        with ``extractOnly=False`` if you want to insert with no additional
        processing.

        Returns None if metadata cannot be extracted; otherwise returns a
        dictionary containing at least two keys:

            :contents:
                        Extracted full-text content, if applicable
            :metadata:
                        key:value pairs of text strings
        """
        if not hasattr(file_obj, "name"):
            raise ValueError("extract() requires file-like objects which have a defined name property")

        params = {
            "extractOnly": "true" if extractOnly else "false",
            "lowernames": "true",
            "wt": "json",
        }
        params.update(kwargs)

        try:
            # We'll provide the file using its true name as Tika may use that
            # as a file type hint:
            resp = await self._send_request(
                'post', 'update/extract',
                body=params,
                files=[file_obj])
        except (IOError, SolrError) as err:
            self.log.error("Failed to extract document metadata: %s", err,
                           exc_info=True)
            raise

        try:
            data = json.loads(resp)
        except ValueError as err:
            self.log.error("Failed to load JSON response: %s", err,
                           exc_info=True)
            raise

        data['contents'] = data.pop(file_obj.name, None)
        data['metadata'] = metadata = {}

        raw_metadata = data.pop("%s_metadata" % file_obj.name, None)

        if raw_metadata:
            # The raw format is somewhat annoying: it's a flat list of
            # alternating keys and value lists
            while raw_metadata:
                metadata[raw_metadata.pop()] = raw_metadata.pop()

        return data

    def close(self):
        self.session.close()
