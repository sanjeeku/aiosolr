class Results(object):

    """
    Default results class for wrapping decoded (from JSON) solr responses.

    Required ``decoded`` argument must be a Solr response dictionary.
    Individual documents can be retrieved either through ``docs`` attribute
    or by iterating over results instance.

    Example::

        results = Results({
            'response': {
                'docs': [{'id': 1}, {'id': 2}, {'id': 3}],
                'numFound': 3,
            }
        })

        # this:
        for doc in results:
            print doc

        # ... is equivalent to:
        for doc in results.docs:
            print doc

        # also:
        list(results) == results.docs

    Note that ``Results`` object does not support indexing and slicing. If you
    need to retrieve documents by index just use ``docs`` attribute.

    Other response metadata (debug, highlighting, qtime, etc.) are available
    as attributes. Note that not all response keys may be covered for current
    version of aiosolr. If you're sure that your queries return
    something that is missing you can easily extend ``Results``
    and provide it as a custom results class to ``aiosolr.Solr``.

    Example::

        import aiosolr

        class CustomResults(aiosolr.Results):
            def __init__(self, decoded):
                 self.some_new_attribute = decoded.get('not_covered_key' None)
                 super(self, CustomResults).__init__(decoded)

        solr = Solr('<solr url>', response_cls=CustomResults)

    """

    def __init__(self, decoded):
        # main response part of decoded Solr response
        response_part = decoded.get('response') or {}
        self.docs = response_part.get('docs', ())
        self.hits = response_part.get('numFound', 0)

        # other response metadata
        self.debug = decoded.get('debug', {})
        self.highlighting = decoded.get('highlighting', {})
        self.facets = decoded.get('facet_counts', {})
        self.spellcheck = decoded.get('spellcheck', {})
        self.stats = decoded.get('stats', {})
        self.qtime = decoded.get('responseHeader', {}).get('QTime', None)
        self.grouped = decoded.get('grouped', {})
        self.nextCursorMark = decoded.get('nextCursorMark', None)

    def __len__(self):
        return len(self.docs)

    def __iter__(self):
        return iter(self.docs)
