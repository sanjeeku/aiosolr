# coding: utf-8
import re
import asyncio
from xml.etree import ElementTree
from .log import LOG
from .utils import force_unicode, unescape_html


def scrape_response(headers, response):
    """
    Scrape the html response.
    """
    # identify the responding server
    server_type = None
    server_string = headers.get('server', '')

    if server_string and 'jetty' in server_string.lower():
        server_type = 'jetty'

    if server_string and 'coyote' in server_string.lower():
        server_type = 'tomcat'

    reason = None
    full_html = ''
    dom_tree = None

    # In Python3, response can be made of bytes
    if hasattr(response, 'decode'):
        response = response.decode()
    if response.startswith('<?xml'):
        # Try a strict XML parse
        try:
            soup = ElementTree.fromstring(response)

            reason_node = soup.find('lst[@name="error"]/str[@name="msg"]')
            tb_node = soup.find('lst[@name="error"]/str[@name="trace"]')
            if reason_node is not None:
                full_html = reason = reason_node.text.strip()
            if tb_node is not None:
                full_html = tb_node.text.strip()
                if reason is None:
                    reason = full_html

            # Since we had a precise match, we'll return the results now:
            if reason and full_html:
                return reason, full_html
        except ElementTree.ParseError:
            # XML parsing error, so we'll let the more liberal code handle it.
            pass

    if server_type == 'tomcat':
        # Tomcat doesn't produce a valid XML response or consistent HTML:
        m = re.search(r'<(h1)[^>]*>\s*(.+?)\s*</\1>', response, re.IGNORECASE)
        if m:
            reason = m.group(2)
        else:
            full_html = "%s" % response
    else:
        # Let's assume others do produce a valid XML response
        try:
            dom_tree = ElementTree.fromstring(response)
            reason_node = None

            # html page might be different for every server
            if server_type == 'jetty':
                reason_node = dom_tree.find('body/pre')
            else:
                reason_node = dom_tree.find('head/title')

            if reason_node is not None:
                reason = reason_node.text

            if reason is None:
                full_html = ElementTree.tostring(dom_tree)
        except SyntaxError as err:
            LOG.warning('Unable to extract error message from invalid XML: %s', err,
                        extra={'data': {'response': response}})
            full_html = "%s" % response

    full_html = force_unicode(full_html)
    full_html = full_html.replace('\n', '')
    full_html = full_html.replace('\r', '')
    full_html = full_html.replace('<br/>', '')
    full_html = full_html.replace('<br />', '')
    full_html = full_html.strip()
    return reason, full_html


def make_error_msg(reason, full_response):
    msg = "[Reason: %s]" % reason
    if reason is None:
        msg += "\n%s" % full_response
    return msg


@asyncio.coroutine
def extract_error(resp):
    full_response = reason = None
    try:
        # if response is in json format
        json_data = yield from resp.json()
        reason = json_data['error']['msg']
    except KeyError:
        # if json response has unexpected structure
        full_response = resp.content
    except ValueError:
        # otherwise we assume it's html
        response_text = yield from resp.read()
        reason, full_html = scrape_response(resp.headers, response_text)
        full_response = unescape_html(full_html)
    return reason, full_response
