# coding: utf-8
import re
import ast
import datetime
import html.entities as htmlentities


DATETIME_REGEX = re.compile('^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})(\.\d+)?Z$')


def force_bytes(value):
    """
    Forces a Unicode string to become a bytestring.
    """
    if isinstance(value, str):
        value = value.encode('utf-8', 'backslashreplace')
    return value


def force_unicode(value):
    """
    Forces a bytestring to become a Unicode string.
    """
    if isinstance(value, bytes):
        value = value.decode('utf-8', errors='replace')
    elif not isinstance(value, str):
        value = str(value)
    return value


def unescape_html(text):
    """
    Removes HTML or XML character references and entities from a text string.

    @param text The HTML (or XML) source text.
    @return The plain text, as a Unicode string, if necessary.

    Source: http://effbot.org/zone/re-sub.htm#unescape-html
    """
    def fixup(m):
        text = m.group(0)
        if text[:2] == "&#":
            # character reference
            try:
                if text[:3] == "&#x":
                    return chr(int(text[3:-1], 16))
                else:
                    return chr(int(text[2:-1]))
            except ValueError:
                pass
        else:
            # named entity
            try:
                text = chr(htmlentities.name2codepoint[text[1:-1]])
            except KeyError:
                pass
        return text  # leave as is
    return re.sub("&#?\w+;", fixup, text)


def is_valid_xml_char_ordinal(i):
    """
    Defines whether char is valid to use in xml document

    XML standard defines a valid char as::

    Char ::= #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]
    """
    # conditions ordered by presumed frequency
    return (
        0x20 <= i <= 0xD7FF
        or i in (0x9, 0xA, 0xD)
        or 0xE000 <= i <= 0xFFFD
        or 0x10000 <= i <= 0x10FFFF
        )


def clean_xml_string(s):
    """
    Cleans string from invalid xml chars

    Solution was found there::

    http://stackoverflow.com/questions/8733233/filtering-out-certain-bytes-in-python
    """
    return ''.join(c for c in s if is_valid_xml_char_ordinal(ord(c)))


# Using two-tuples to preserve order.
REPLACEMENTS = (
    # Nuke nasty control characters.
    (b'\x00', b''),  # Start of heading
    (b'\x01', b''),  # Start of heading
    (b'\x02', b''),  # Start of text
    (b'\x03', b''),  # End of text
    (b'\x04', b''),  # End of transmission
    (b'\x05', b''),  # Enquiry
    (b'\x06', b''),  # Acknowledge
    (b'\x07', b''),  # Ring terminal bell
    (b'\x08', b''),  # Backspace
    (b'\x0b', b''),  # Vertical tab
    (b'\x0c', b''),  # Form feed
    (b'\x0e', b''),  # Shift out
    (b'\x0f', b''),  # Shift in
    (b'\x10', b''),  # Data link escape
    (b'\x11', b''),  # Device control 1
    (b'\x12', b''),  # Device control 2
    (b'\x13', b''),  # Device control 3
    (b'\x14', b''),  # Device control 4
    (b'\x15', b''),  # Negative acknowledge
    (b'\x16', b''),  # Synchronous idle
    (b'\x17', b''),  # End of transmission block
    (b'\x18', b''),  # Cancel
    (b'\x19', b''),  # End of medium
    (b'\x1a', b''),  # Substitute character
    (b'\x1b', b''),  # Escape
    (b'\x1c', b''),  # File separator
    (b'\x1d', b''),  # Group separator
    (b'\x1e', b''),  # Record separator
    (b'\x1f', b''),  # Unit separator
)


def sanitize(data):
    fixed_string = force_bytes(data)

    for bad, good in REPLACEMENTS:
        fixed_string = fixed_string.replace(bad, good)

    return force_unicode(fixed_string)


def is_null_value(value):
    """
    Check if a given value is ``null``.

    Criteria for this is based on values that shouldn't be included
    in the Solr ``add`` request at all.
    """
    if value is None:
        return True

    if isinstance(value, str) and len(value) == 0:
        return True

    # TODO: This should probably be removed when solved in core Solr level?
    return False


def from_python(value):
    """
    Converts python values to a form suitable for insertion into the xml
    we send to solr.
    """
    if hasattr(value, 'strftime'):
        if hasattr(value, 'hour'):
            value = "%sZ" % value.isoformat()
        else:
            value = "%sT00:00:00Z" % value.isoformat()
    elif isinstance(value, bool):
        if value:
            value = 'true'
        else:
            value = 'false'
    else:
        if isinstance(value, bytes):
            value = str(value, errors='replace')
        value = "{0}".format(value)
    return clean_xml_string(value)


def to_python(value):
    """
    Converts values from Solr to native Python values.
    """
    if isinstance(value, (int, float, complex)):
        return value

    if isinstance(value, (list, tuple)):
        value = value[0]

    if value == 'true':
        return True
    elif value == 'false':
        return False

    is_string = False

    if isinstance(value, bytes):
        value = force_unicode(value)

    if isinstance(value, str):
        is_string = True

    if is_string:
        possible_datetime = DATETIME_REGEX.search(value)

        if possible_datetime:
            date_values = possible_datetime.groupdict()

            for dk, dv in date_values.items():
                date_values[dk] = int(dv)

            return datetime.datetime(date_values['year'], date_values['month'], date_values['day'], date_values['hour'], date_values['minute'], date_values['second'])

    try:
        # This is slightly gross but it's hard to tell otherwise what the
        # string's original type might have been.
        return ast.literal_eval(value)
    except (ValueError, SyntaxError):
        # If it fails, continue on.
        pass

    return value
