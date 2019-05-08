# coding: utf-8
import gc
import json
import datetime
import unittest
import asyncio
from io import BytesIO
from xml.etree import ElementTree
from aiosolr import Solr, SolrError
from aiosolr.result_cls import Results
from aiosolr.utils import (
    clean_xml_string, force_bytes, force_unicode, sanitize, unescape_html)
from aiosolr.error_extractor import (
    extract_error, make_error_msg, scrape_response)


class BaseAIOTestCase(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.solr.close()
        self.loop.close()
        self.loop = None
        gc.collect()


class UtilsTestCase(unittest.TestCase):

    def test_unescape_html(self):
        self.assertEqual(unescape_html('Hello &#149; world'), 'Hello \x95 world')
        self.assertEqual(unescape_html('Hello &#x64; world'), 'Hello d world')
        self.assertEqual(unescape_html('Hello &amp; ☃'), 'Hello & ☃')
        self.assertEqual(unescape_html('Hello &doesnotexist; world'), 'Hello &doesnotexist; world')

    def test_sanitize(self):
        self.assertEqual(sanitize('\x00\x01\x02\x03\x04\x05\x06\x07\x08\x0b\x0c\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19h\x1ae\x1bl\x1cl\x1do\x1e\x1f'), 'hello'),

    def test_force_unicode(self):
        self.assertEqual(force_unicode(b'Hello \xe2\x98\x83'), 'Hello ☃')
        # Don't mangle, it's already Unicode.
        self.assertEqual(force_unicode('Hello ☃'), 'Hello ☃')

        self.assertEqual(force_unicode(1), '1', "force_unicode() should convert ints")
        self.assertEqual(force_unicode(1.0), '1.0', "force_unicode() should convert floats")
        self.assertEqual(force_unicode(None), 'None', 'force_unicode() should convert None')

    def test_force_bytes(self):
        self.assertEqual(force_bytes('Hello ☃'), b'Hello \xe2\x98\x83')
        # Don't mangle, it's already a bytestring.
        self.assertEqual(force_bytes(b'Hello \xe2\x98\x83'), b'Hello \xe2\x98\x83')

    def test_clean_xml_string(self):
        self.assertEqual(clean_xml_string('\x00\x0b\x0d\uffff'), '\x0d')


class ResultsTestCase(unittest.TestCase):

    def test_init(self):
        default_results = Results({
            'response': {
                'docs': [{'id': 1}, {'id': 2}],
                'numFound': 2,
            },
        })

        self.assertEqual(default_results.docs, [{'id': 1}, {'id': 2}])
        self.assertEqual(default_results.hits, 2)
        self.assertEqual(default_results.highlighting, {})
        self.assertEqual(default_results.facets, {})
        self.assertEqual(default_results.spellcheck, {})
        self.assertEqual(default_results.stats, {})
        self.assertEqual(default_results.qtime, None)
        self.assertEqual(default_results.debug, {})
        self.assertEqual(default_results.grouped, {})

        full_results = Results({
            'response': {
                'docs': [{'id': 1}, {'id': 2}, {'id': 3}],
                'numFound': 3,
            },
            # Fake data just to check assignments.
            'highlighting': 'hi',
            'facet_counts': 'fa',
            'spellcheck': 'sp',
            'stats': 'st',
            'responseHeader': {
                'QTime': '0.001',
            },
            'debug': True,
            'grouped': ['a'],
        })

        self.assertEqual(full_results.docs, [{'id': 1}, {'id': 2}, {'id': 3}])
        self.assertEqual(full_results.hits, 3)
        self.assertEqual(full_results.highlighting, 'hi')
        self.assertEqual(full_results.facets, 'fa')
        self.assertEqual(full_results.spellcheck, 'sp')
        self.assertEqual(full_results.stats, 'st')
        self.assertEqual(full_results.qtime, '0.001')
        self.assertEqual(full_results.debug, True)
        self.assertEqual(full_results.grouped, ['a'])

    def test_len(self):
        small_results = Results({
            'response': {
                'docs': [{'id': 1}, {'id': 2}],
                'numFound': 2,
            },
        })
        self.assertEqual(len(small_results), 2)

        wrong_hits_results = Results({
            'response': {
                'docs': [{'id': 1}, {'id': 2}, {'id': 3}],
                'numFound': 7,
            },
        })
        self.assertEqual(len(wrong_hits_results), 3)

    def test_iter(self):
        long_results = Results({
            'response': {
                'docs': [{'id': 1}, {'id': 2}, {'id': 3}],
                'numFound': 7,
            },
        })

        to_iter = list(long_results)
        self.assertEqual(to_iter[0], {'id': 1})
        self.assertEqual(to_iter[1], {'id': 2})
        self.assertEqual(to_iter[2], {'id': 3})


class SolrTestCase(BaseAIOTestCase):

    def setUp(self):
        super(SolrTestCase, self).setUp()
        self.solr = self.get_solr("core0")
        self.docs = [
            {
                'id': 'doc_1',
                'title': 'Example doc 1',
                'price': 12.59,
                'popularity': 10,
            },
            {
                'id': 'doc_2',
                'title': 'Another example ☃ doc 2',
                'price': 13.69,
                'popularity': 7,
            },
            {
                'id': 'doc_3',
                'title': 'Another thing',
                'price': 2.35,
                'popularity': 8,
            },
            {
                'id': 'doc_4',
                'title': 'doc rock',
                'price': 99.99,
                'popularity': 10,
            },
            {
                'id': 'doc_5',
                'title': 'Boring',
                'price': 1.12,
                'popularity': 2,
            },
        ]


        async def init():
            # Clear it.
            await self.solr.delete(q='*:*')
            # Index our docs. Yes, this leans on functionality we're going to test
            # later & if it's broken, everything will catastrophically fail.
            # Such is life.
            await self.solr.add(self.docs)

        self.loop.run_until_complete(init())

    def assertURLStartsWith(self, URL, path):
        """Assert that the test URL provided starts with a known base and the provided path"""
        # Note that we do not use urljoin to ensure that any changes in trailing
        # slash handling are caught quickly:
        return self.assertEqual(URL, '%s/%s' % (self.solr.url.replace('/core0', ''), path))

    def get_solr(self, collection, timeout=60):
        return Solr(
            'http://localhost:8983/solr/%s' % collection,
            timeout=timeout,
            loop=self.loop)

    def test_init(self):
        self.assertEqual(self.solr.url, 'http://localhost:8983/solr/core0')
        self.assertTrue(isinstance(self.solr.decoder, json.JSONDecoder))
        self.assertEqual(self.solr.timeout, 60)

        custom_solr = self.get_solr("core0", timeout=17)
        self.assertEqual(custom_solr.timeout, 17)
        custom_solr.close()

    def test_custom_results_class(self):
        solr = Solr(
            'http://localhost:8983/solr/core0',
            results_cls=dict,
            loop=self.loop)

        results = self.loop.run_until_complete(solr.search(q='*:*'))
        solr.close()
        assert isinstance(results, dict)
        assert 'responseHeader' in results
        assert 'response' in results

    def test__create_full_url_base(self):
        self.assertURLStartsWith(self.solr._create_full_url(path=''),
                                 'core0')

    def test__create_full_url_with_path(self):
        self.assertURLStartsWith(self.solr._create_full_url(path='pysolr_tests'),
                                 'core0/pysolr_tests')

    def test__create_full_url_with_path_and_querystring(self):
        # Note the use of a querystring parameter including a trailing slash to catch sloppy trimming:
        self.assertURLStartsWith(self.solr._create_full_url(path='/pysolr_tests/select/?whatever=/'),
                                 'core0/pysolr_tests/select/?whatever=/')

    def test__send_request(self):
        # Test a valid request.
        resp_body = self.loop.run_until_complete(self.solr._send_request('GET', 'select/?q=doc&wt=json'))
        self.assertTrue('"numFound":3' in resp_body)

        # Test a lowercase method & a body.
        xml_body = '<add><doc><field name="id">doc_12</field><field name="title">Whee! ☃</field></doc></add>'
        resp_body = self.loop.run_until_complete(self.solr._send_request('POST', 'update/?commit=true', body=xml_body, headers={
            'Content-type': 'text/xml; charset=utf-8',
        }))
        self.assertTrue('<int name="status">0</int>' in resp_body)

    def test__send_request_to_bad_path(self):
        # Test a non-existent URL:
        self.solr.url = 'http://127.0.0.1:567898/wahtever'
        with self.assertRaises(SolrError):
            self.loop.run_until_complete(self.solr._send_request('get', 'select/?q=doc&wt=json'))

    def test_send_request_to_bad_core(self):
        # Test a bad core on a valid URL:
        self.solr.url = 'http://localhost:8983/solr/bad_core'
        with self.assertRaises(SolrError):
            self.loop.run_until_complete(self.solr._send_request('get', 'select/?q=doc&wt=json'))

    def test__select(self):
        # Short params.
        resp_body = self.loop.run_until_complete(self.solr._select({'q': 'doc'}))
        resp_data = json.loads(resp_body)
        self.assertEqual(resp_data['response']['numFound'], 3)

        # Long params.
        resp_body = self.loop.run_until_complete(self.solr._select({'q': 'doc' * 1024}))
        resp_data = json.loads(resp_body)
        self.assertEqual(resp_data['response']['numFound'], 0)
        self.assertEqual(len(resp_data['responseHeader']['params']['q']), 3 * 1024)

        # Test Deep Pagination CursorMark
        resp_body = self.loop.run_until_complete(
            self.solr._select({'q': '*', 'cursorMark': '*', 'sort': 'id desc', 'start': 0, 'rows': 2}))
        resp_data = json.loads(resp_body)
        self.assertEqual(len(resp_data['response']['docs']), 2)
        self.assertIn('nextCursorMark', resp_data)

    def test__mlt(self):
        resp_body = self.loop.run_until_complete(self.solr._mlt({'q': 'id:doc_1', 'mlt.fl': 'title'}))
        resp_data = json.loads(resp_body)
        self.assertEqual(resp_data['response']['numFound'], 0)

    def test__suggest_terms(self):
        resp_body = self.loop.run_until_complete(self.solr._select({'terms.fl': 'title'}))
        resp_data = json.loads(resp_body)
        self.assertEqual(resp_data['response']['numFound'], 0)

    def test__update(self):
        xml_body = '<add><doc><field name="id">doc_12</field><field name="title">Whee!</field></doc></add>'
        resp_body = self.loop.run_until_complete(self.solr._update(xml_body))
        self.assertTrue('<int name="status">0</int>' in resp_body)

    def test__soft_commit(self):
        xml_body = '<add><doc><field name="id">doc_12</field><field name="title">Whee!</field></doc></add>'
        resp_body = self.loop.run_until_complete(self.solr._update(xml_body, softCommit=True))
        self.assertTrue('<int name="status">0</int>' in resp_body)

    def test__extract_error(self):
        class RubbishResponse(object):

            def __init__(self, content, headers=None, loop=None):
                self.loop = loop
                if isinstance(content, bytes):
                    content = content.decode('utf-8')
                self.content = content
                self.headers = headers

                if self.headers is None:
                    self.headers = {}

            async def json(self):
                await asyncio.sleep(0.01, loop=self.loop)
                return json.loads(self.content)

            async def read(self):
                await asyncio.sleep(0.01, loop=self.loop)
                return self.content

        # Just the reason.
        reason = 'Something went wrong.'
        full_response = None
        self.assertEqual(
            make_error_msg(reason, full_response),
            "[Reason: Something went wrong.]")

        # Empty reason.
        resp_2 = RubbishResponse(
            "We don't care.",
            {'reason': None},
            loop=self.loop)
        reason, full_response = self.loop.run_until_complete(extract_error(resp_2))
        self.assertEqual(
            make_error_msg(reason, full_response),
            "[Reason: None]\nWe don't care.")

        # No reason. Time to scrape.
        resp_3 = RubbishResponse(
            '<html><body><pre>Something is broke.</pre></body></html>',
            {'server': 'jetty'},
            loop=self.loop)
        reason, full_response = self.loop.run_until_complete(
            extract_error(resp_3))
        self.assertEqual(
            make_error_msg(reason, full_response),
            "[Reason: Something is broke.]")

        # No reason. JSON response.
        resp_4 = RubbishResponse(
            b'\n {"error": {"msg": "It happens"}}',
            {'server': 'tomcat'},
            loop=self.loop)
        reason, full_response = self.loop.run_until_complete(extract_error(resp_4))
        self.assertEqual(
            make_error_msg(reason, full_response),
            "[Reason: It happens]")

        # No reason. Weird JSON response.
        resp_5 = RubbishResponse(
            b'{"kinda": "weird"}',
            {'server': 'jetty'},
            loop=self.loop)
        reason, full_response = self.loop.run_until_complete(extract_error(resp_5))
        self.assertEqual(
            make_error_msg(reason, full_response),
            '[Reason: None]\n{"kinda": "weird"}')

    def test__scrape_response(self):
        # Jetty.
        resp_1 = scrape_response({'server': 'jetty'}, '<html><body><pre>Something is broke.</pre></body></html>')
        self.assertEqual(resp_1, ('Something is broke.', u''))

        # Other.
        resp_2 = scrape_response({'server': 'crapzilla'}, '<html><head><title>Wow. Seriously weird.</title></head><body><pre>Something is broke.</pre></body></html>')
        self.assertEqual(resp_2, ('Wow. Seriously weird.', u''))

    def test__scrape_response_coyote_xml(self):
        resp_3 = scrape_response({'server': 'coyote'}, '<?xml version="1.0"?>\n<response>\n<lst name="responseHeader"><int name="status">400</int><int name="QTime">0</int></lst><lst name="error"><str name="msg">Invalid Date String:\'2015-03-23 10:43:33\'</str><int name="code">400</int></lst>\n</response>\n')
        self.assertEqual(resp_3, ("Invalid Date String:'2015-03-23 10:43:33'", "Invalid Date String:'2015-03-23 10:43:33'"))

        # Valid XML with a traceback
        resp_4 = scrape_response({'server': 'coyote'}, """<?xml version="1.0"?>
<response>
<lst name="responseHeader"><int name="status">500</int><int name="QTime">138</int></lst><lst name="error"><str name="msg">Internal Server Error</str><str name="trace">org.apache.solr.common.SolrException: Internal Server Error at java.lang.Thread.run(Thread.java:745)</str><int name="code">500</int></lst>
</response>""")
        self.assertEqual(resp_4, (u"Internal Server Error", u"org.apache.solr.common.SolrException: Internal Server Error at java.lang.Thread.run(Thread.java:745)"))

    def test__scrape_response_tomcat(self):
        """Tests for Tomcat error responses"""

        resp_0 = scrape_response({'server': 'coyote'}, '<html><body><h1>Something broke!</h1><pre>gigantic stack trace</pre></body></html>')
        self.assertEqual(resp_0, ('Something broke!', ''))

        # Invalid XML
        bogus_xml = '<?xml version="1.0"?>\n<response>\n<lst name="responseHeader"><int name="status">400</int><int name="QTime">0</int></lst><lst name="error"><str name="msg">Invalid Date String:\'2015-03-23 10:43:33\'</str><int name="code">400</int></lst>'
        reason, full_html = scrape_response({'server': 'coyote'}, bogus_xml)
        self.assertEqual(reason, None)
        self.assertEqual(full_html, bogus_xml.replace("\n", ""))

    def test__from_python(self):
        self.assertEqual(self.solr._from_python(datetime.date(2013, 1, 18)), '2013-01-18T00:00:00Z')
        self.assertEqual(self.solr._from_python(datetime.datetime(2013, 1, 18, 0, 30, 28)), '2013-01-18T00:30:28Z')
        self.assertEqual(self.solr._from_python(True), 'true')
        self.assertEqual(self.solr._from_python(False), 'false')
        self.assertEqual(self.solr._from_python(1), '1')
        self.assertEqual(self.solr._from_python(1.2), '1.2')
        self.assertEqual(self.solr._from_python(b'hello'), 'hello')
        self.assertEqual(self.solr._from_python('hello ☃'), 'hello ☃')
        self.assertEqual(self.solr._from_python('\x01test\x02'), 'test')

    def test__to_python(self):
        self.assertEqual(self.solr._to_python('2013-01-18T00:00:00Z'), datetime.datetime(2013, 1, 18))
        self.assertEqual(self.solr._to_python('2013-01-18T00:30:28Z'), datetime.datetime(2013, 1, 18, 0, 30, 28))
        self.assertEqual(self.solr._to_python('true'), True)
        self.assertEqual(self.solr._to_python('false'), False)
        self.assertEqual(self.solr._to_python(1), 1)
        self.assertEqual(self.solr._to_python(1.2), 1.2)
        self.assertEqual(self.solr._to_python(b'hello'), 'hello')
        self.assertEqual(self.solr._to_python('hello ☃'), 'hello ☃')
        self.assertEqual(self.solr._to_python(['foo', 'bar']), 'foo')
        self.assertEqual(self.solr._to_python(('foo', 'bar')), 'foo')
        self.assertEqual(self.solr._to_python('tuple("foo", "bar")'), 'tuple("foo", "bar")')

    def test__is_null_value(self):
        self.assertTrue(self.solr._is_null_value(None))
        self.assertTrue(self.solr._is_null_value(''))

        self.assertFalse(self.solr._is_null_value('Hello'))
        self.assertFalse(self.solr._is_null_value(1))

    def test_search(self):
        results = self.loop.run_until_complete(self.solr.search('doc'))
        self.assertEqual(len(results), 3)

        results = self.loop.run_until_complete(self.solr.search('example'))
        self.assertEqual(len(results), 2)

        results = self.loop.run_until_complete(self.solr.search('nothing'))
        self.assertEqual(len(results), 0)

        # Advanced options.
        results = self.loop.run_until_complete(self.solr.search('doc', **{
            'debug': 'true',
            'hl': 'true',
            'hl.fragsize': 8,
            'facet': 'on',
            'facet.field': 'popularity',
            'spellcheck': 'true',
            'spellcheck.collate': 'true',
            'spellcheck.count': 1,
            # TODO: Can't get these working in my test setup.
            # 'group': 'true',
            # 'group.field': 'id',
        }))
        self.assertEqual(len(results), 3)
        self.assertTrue('explain' in results.debug)
        self.assertEqual(results.highlighting, {u'doc_4': {}, u'doc_2': {}, u'doc_1': {}})
        self.assertEqual(results.spellcheck, {})
        self.assertEqual(results.facets['facet_fields']['popularity'], ['10', 2, '7', 1, '2', 0, '8', 0])
        self.assertTrue(results.qtime is not None)
        # TODO: Can't get these working in my test setup.
        # self.assertEqual(results.grouped, '')

    def test_multiple_search_handlers(self):
        misspelled_words = 'anthr thng'
        # By default, the 'select' search handler should be used
        results = self.loop.run_until_complete(
            self.solr.search(q=misspelled_words))
        self.assertEqual(results.spellcheck, {})
        # spell search handler should return suggestions
        # NB: this test relies on the spell search handler in the
        # solrconfig (see the SOLR_ARCHIVE used by the start-solr-test-server script)
        results = self.loop.run_until_complete(
            self.solr.search(q=misspelled_words, search_handler='spell'))
        self.assertNotEqual(results.spellcheck, {})

    def test_more_like_this(self):
        results = self.loop.run_until_complete(
            self.solr.more_like_this('id:doc_1', 'text'))
        self.assertEqual(len(results), 0)

    def test_suggest_terms(self):
        results = self.loop.run_until_complete(self.solr.suggest_terms('title', ''))
        self.assertEqual(len(results), 1)
        self.assertEqual(results, {'title': [('doc', 3), ('another', 2), ('example', 2), ('1', 1), ('2', 1), ('boring', 1), ('rock', 1), ('thing', 1)]})

    def test__build_doc(self):
        doc = {
            'id': 'doc_1',
            'title': 'Example doc ☃ 1',
            'price': 12.59,
            'popularity': 10,
        }
        doc_xml = force_unicode(ElementTree.tostring(self.solr._build_doc(doc), encoding='utf-8'))
        self.assertTrue('<field name="title">Example doc ☃ 1</field>' in doc_xml)
        self.assertTrue('<field name="id">doc_1</field>' in doc_xml)
        self.assertEqual(len(doc_xml), 152)

    def test_add(self):
        res1 = self.loop.run_until_complete(self.solr.search('doc'))
        self.assertEqual(len(res1), 3)
        res2 = self.loop.run_until_complete(self.solr.search('example'))
        self.assertEqual(len(res2), 2)

        self.loop.run_until_complete(self.solr.add([
            {
                'id': 'doc_6',
                'title': 'Newly added doc',
            },
            {
                'id': 'doc_7',
                'title': 'Another example doc',
            },
        ]))

        res1 = self.loop.run_until_complete(self.solr.search('doc'))
        self.assertEqual(len(res1), 5)
        res2 = self.loop.run_until_complete(self.solr.search('example'))
        self.assertEqual(len(res2), 3)

    def test_add_with_boost(self):
        res1 = self.loop.run_until_complete(self.solr.search('doc'))
        self.assertEqual(len(res1), 3)

        self.loop.run_until_complete(
            self.solr.add(
                [{'id': 'doc_6', 'title': 'Important doc'}],
                boost={'title': 10.0}))

        self.loop.run_until_complete(
            self.solr.add(
                [{'id': 'doc_7', 'title': 'Spam doc doc'}],
                boost={'title': 0}))

        res = self.loop.run_until_complete(self.solr.search('doc'))
        self.assertEqual(len(res), 5)
        self.assertEqual('doc_6', res.docs[0]['id'])

    def test_field_update(self):
        originalDocs = self.loop.run_until_complete(self.solr.search('doc'))
        self.assertEqual(len(originalDocs), 3)
        updateList = []
        for i, doc in enumerate(originalDocs):
            updateList.append({'id': doc['id'], 'popularity': 5})
        self.loop.run_until_complete(
            self.solr.add(updateList, fieldUpdates={'popularity': 'inc'}))

        updatedDocs = self.loop.run_until_complete(self.solr.search('doc'))
        self.assertEqual(len(updatedDocs), 3)
        for i, (originalDoc, updatedDoc) in enumerate(zip(originalDocs, updatedDocs)):
            self.assertEqual(len(updatedDoc.keys()), len(originalDoc.keys()))
            self.assertEqual(updatedDoc['popularity'], originalDoc['popularity'] + 5)
            # TODO: change this to use assertSetEqual:
            self.assertEqual(True, all(updatedDoc[k] == originalDoc[k] for k in updatedDoc.keys()
                                       if k not in ['_version_', 'popularity']))

        self.loop.run_until_complete(
            self.solr.add([
                {
                    'id': 'multivalued_1',
                    'title': 'Multivalued doc 1',
                    'word_ss': ['alpha', 'beta'],
                },
                {
                    'id': 'multivalued_2',
                    'title': 'Multivalued doc 2',
                    'word_ss': ['charlie', 'delta'],
                },
            ]))

        originalDocs = self.loop.run_until_complete(
            self.solr.search('multivalued'))
        self.assertEqual(len(originalDocs), 2)
        updateList = []
        for i, doc in enumerate(originalDocs):
            updateList.append({'id': doc['id'], 'word_ss': ['epsilon', 'gamma']})
        self.loop.run_until_complete(
            self.solr.add(updateList, fieldUpdates={'word_ss': 'add'}))

        updatedDocs = self.loop.run_until_complete(
            self.solr.search('multivalued'))
        self.assertEqual(len(updatedDocs), 2)
        for i, (originalDoc, updatedDoc) in enumerate(zip(originalDocs, updatedDocs)):
            self.assertEqual(len(updatedDoc.keys()), len(originalDoc.keys()))
            self.assertEqual(updatedDoc['word_ss'], originalDoc['word_ss'] + ['epsilon', 'gamma'])
            # TODO: change this to use assertSetEqual:
            self.assertEqual(True, all(updatedDoc[k] == originalDoc[k] for k in updatedDoc.keys()
                                       if k not in ['_version_', 'word_ss']))

    def test_delete(self):
        self.assertEqual(
            len(self.loop.run_until_complete(self.solr.search('doc'))), 3)
        self.loop.run_until_complete(self.solr.delete(id='doc_1'))
        self.assertEqual(
            len(self.loop.run_until_complete(self.solr.search('doc'))), 2)
        self.loop.run_until_complete(self.solr.delete(q='price:[0 TO 15]'))
        self.assertEqual(
            len(self.loop.run_until_complete(self.solr.search('doc'))), 1)

        self.assertEqual(
            len(self.loop.run_until_complete(self.solr.search('*:*'))), 1)
        self.loop.run_until_complete(self.solr.delete(q='*:*'))
        self.assertEqual(
            len(self.loop.run_until_complete(self.solr.search('*:*'))), 0)

        # Need at least one.
        with self.assertRaises(ValueError):
            self.loop.run_until_complete(self.solr.delete())
        # Can't have both.
        with self.assertRaises(ValueError):
            self.loop.run_until_complete(self.solr.delete(id='foo', q='bar'))

    def test_commit(self):
        self.assertEqual(
            len(self.loop.run_until_complete(self.solr.search('doc'))), 3)
        self.loop.run_until_complete(
            self.solr.add([
                {
                    'id': 'doc_6',
                    'title': 'Newly added doc',
                }
            ], commit=False))
        self.assertEqual(
            len(self.loop.run_until_complete(self.solr.search('doc'))), 3)
        self.loop.run_until_complete(self.solr.commit())
        self.assertEqual(
            len(self.loop.run_until_complete(self.solr.search('doc'))), 4)

    def test_overwrite(self):
        result = self.loop.run_until_complete(
            self.solr.search('id:doc_overwrite_1'))
        self.assertEqual(
            len(result), 0)
        self.loop.run_until_complete(self.solr.add([
            {
                'id': 'doc_overwrite_1',
                'title': 'Kim is awesome.',
            },
            {
                'id': 'doc_overwrite_1',
                'title': 'Kim is more awesome.',
            }
        ], overwrite=False))
        result = self.loop.run_until_complete(self.solr.search('id:doc_overwrite_1'))
        self.assertEqual(len(result), 2)

    def test_optimize(self):
        # Make sure it doesn't blow up. Side effects are hard to measure. :/
        result = self.loop.run_until_complete(self.solr.search('doc'))
        self.assertEqual(len(result), 3)
        self.loop.run_until_complete(self.solr.add([
            {
                'id': 'doc_6',
                'title': 'Newly added doc',
            }
        ], commit=False))
        result = self.loop.run_until_complete(self.solr.search('doc'))
        self.assertEqual(len(result), 3)
        self.loop.run_until_complete(self.solr.optimize())
        result = self.loop.run_until_complete(self.solr.search('doc'))
        self.assertEqual(len(result), 4)

    def test_extract(self):
        fake_f = BytesIO("""
            <html>
                <head>
                    <meta charset="utf-8">
                    <meta name="haystack-test" content="test 1234">
                    <title>Test Title ☃&#x2603;</title>
                </head>
                    <body>foobar</body>
            </html>
        """.encode('utf-8'))
        fake_f.name = "test.html"
        extracted = self.loop.run_until_complete(self.solr.extract(fake_f))

        # Verify documented response structure:
        self.assertIn('contents', extracted)
        self.assertIn('metadata', extracted)

        self.assertIn('foobar', extracted['contents'])

        m = extracted['metadata']

        self.assertEqual([fake_f.name], m['stream_name'])

        self.assertIn('haystack-test', m, "HTML metadata should have been extracted!")
        self.assertEqual(['test 1234'], m['haystack-test'])

        # Note the underhanded use of a double snowman to verify both that Tika
        # correctly decoded entities and that our UTF-8 characters survived the
        # round-trip:
        self.assertEqual(['Test Title ☃☃'], m['title'])

    def test_full_url(self):
        self.solr.url = 'http://localhost:8983/solr/core0'
        full_url = self.solr._create_full_url(path='/update')

        # Make sure trailing and leading slashes do not collide:
        self.assertEqual(full_url, 'http://localhost:8983/solr/core0/update')
