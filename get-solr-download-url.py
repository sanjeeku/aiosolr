# coding: utf-8
import sys
import json
from urllib.request import urlopen
from urllib.parse import urljoin


if len(sys.argv) != 2:
    print('Usage: %s SOLR_VERSION' % sys.argv[0], file=sys.stderr)
    sys.exit(1)

solr_version = sys.argv[1]
tarball = 'solr-{0}.tgz'.format(solr_version)
dist_path = 'lucene/solr/{0}/{1}'.format(solr_version, tarball)

download_url = urljoin('http://archive.apache.org/dist/', dist_path)
mirror_response = urlopen(
    "http://www.apache.org/dyn/mirrors/mirrors.cgi/%s?asjson=1" % dist_path)

if mirror_response.getcode() == 200:
    mirror_data = json.loads(mirror_response.read().decode('utf-8'))
    download_url = urljoin(mirror_data['preferred'], mirror_data['path_info'])
    # The Apache mirror script's response format has recently changed to exclude the actual file paths:
    if not download_url.endswith(tarball):
        download_url = urljoin(download_url, dist_path)

print(download_url)
