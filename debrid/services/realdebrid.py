#import modules
from base import *
from ui.ui_print import *
import releases
import os
import csv
import pprint

# (required) Name of the Debrid service
name = "Real Debrid"
short = "RD"
# (required) Authentification of the Debrid service, can be oauth as well. Create a setting for the required variables in the ui.settings_list. For an oauth example check the trakt authentication.
api_key = ""
# Define Variables
session = requests.Session()
errors = [
    [202, "action already done"],
    [400, "bad Request (see error message)"],
    [403, "permission denied (infringing torrent or account locked or not premium)"],
    [503, "service unavailable (see error message)"],
    [404, "wrong parameter (invalid file id(s)) / unknown resource (invalid id)"],
]

def setup(cls, new=False):
    from debrid.services import setup
    setup(cls, new)

# Error Log
def logerror(response):
    if not response.status_code in [200, 201, 204]:
        desc = ""
        for error in errors:
            if response.status_code == error[0]:
                desc = error[1]
        ui_print("[realdebrid] error: (" + str(response.status_code) + desc + ") " + str(response.content), debug=ui_settings.debug)
    if response.status_code == 401:
        ui_print("[realdebrid] error: (401 unauthorized): realdebrid api key does not seem to work. check your realdebrid settings.")
    if response.status_code == 403:
        ui_print("[realdebrid] error: (403 unauthorized): You may have attempted to add an infringing torrent or your realdebrid account is locked or you don't have premium.")

# Set the CSV file path from environment variable, default to 'catalog.csv'
CSV_FILE_PATH = '/data/catalog.csv'

# Ensure the directory exists
def ensure_directory_exists(file_path):
    directory = os.path.dirname(file_path)
    # print(f"Checking if directory exists: {directory}")
    if directory and not os.path.exists(directory):
        # print(f"Creating directory: {directory}")
        os.makedirs(directory)
    else:
        # print(f"Directory already exists: {directory}")
        pass

# CSV Writing Function
def write_to_csv(data, torrent_file_name, actual_title):
    ensure_directory_exists(CSV_FILE_PATH)
    file_exists = os.path.isfile(CSV_FILE_PATH)
    with open(CSV_FILE_PATH, mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(['EID', 'Title', 'Type', 'Year', 'ParentEID', 'ParentTitle', 'ParentType', 'ParentYear', 'GrandParentEID', 'GrandParentTitle', 'GrandParentType', 'GrandParentYear', 'Torrent File Name', 'Actual Title'])  # header
        row = data + [torrent_file_name, actual_title]
        writer.writerow(row)

# Pretty-print the element object
def print_element_details(element):
    print("Element details:")
    try:
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(vars(element))
    except Exception as e:
        print(f"An error occurred while printing element details: {e}")

# Extract necessary fields from element
def extract_element_data(element):
    eid = element.EID if hasattr(element, 'EID') else ''
    title = element.title if hasattr(element, 'title') else ''
    type_ = element.type if hasattr(element, 'type') else ''
    year = element.year if hasattr(element, 'year') else ''
    parentEID = element.parentEID if hasattr(element, 'parentEID') and element.parentEID else ''
    parentTitle = element.parentTitle if hasattr(element, 'parentTitle') else ''
    parentType = element.parentType if hasattr(element, 'parentType') else ''
    parentYear = element.parentYear if hasattr(element, 'parentYear') else ''
    grandparentEID = element.grandparentEID if hasattr(element, 'grandparentEID') and element.grandparentEID else ''
    grandparentTitle = element.grandparentTitle if hasattr(element, 'grandparentTitle') else ''
    grandparentType = element.grandparentType if hasattr(element, 'grandparentType') else ''
    grandparentYear = element.grandparentYear if hasattr(element, 'grandparentYear') else ''
    
    return [eid, title, type_, year, parentEID, parentTitle, parentType, parentYear, grandparentEID, grandparentTitle, grandparentType, grandparentYear]

# Get Function
def get(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',
        'authorization': 'Bearer ' + api_key
    }
    response = None
    try:
        response = session.get(url, headers=headers)
        logerror(response)
        response = json.loads(response.content, object_hook=lambda d: SimpleNamespace(**d))
    except Exception as e:
        ui_print("[realdebrid] error: (json exception): " + str(e), debug=ui_settings.debug)
        response = None
    return response

# Post Function
def post(url, data):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',
        'authorization': 'Bearer ' + api_key
    }
    response = None
    try:
        response = session.post(url, headers=headers, data=data)
        logerror(response)
        response = json.loads(response.content, object_hook=lambda d: SimpleNamespace(**d))
    except Exception as e:
        if hasattr(response, "status_code"):
            if response.status_code >= 300:
                ui_print("[realdebrid] error: (json exception): " + str(e), debug=ui_settings.debug)
        else:
            ui_print("[realdebrid] error: (json exception): " + str(e), debug=ui_settings.debug)
        response = None
    return response

# Delete Function
def delete(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',
               'authorization': 'Bearer ' + api_key}
    try:
        requests.delete(url, headers=headers)
        # time.sleep(1)
    except Exception as e:
        ui_print("[realdebrid] error: (delete exception): " + str(e), debug=ui_settings.debug)
        None
    return None

# Object classes
class file:
    def __init__(self, id, name, size, wanted_list, unwanted_list):
        self.id = id
        self.name = name
        self.size = size / 1000000000
        self.match = ''
        wanted = False
        unwanted = False
        for key, wanted_pattern in wanted_list:
            if wanted_pattern.search(self.name):
                wanted = True
                self.match = key
                break

        if not wanted:
            for key, unwanted_pattern in unwanted_list:
                if unwanted_pattern.search(self.name) or self.name.endswith('.exe') or self.name.endswith('.txt'):
                    unwanted = True
                    break

        self.wanted = wanted
        self.unwanted = unwanted

    def __eq__(self, other):
        return self.id == other.id

class version:
    def __init__(self, files):
        self.files = files
        self.needed = 0
        self.wanted = 0
        self.unwanted = 0
        self.size = 0
        for file in self.files:
            self.size += file.size
            if file.wanted:
                self.wanted += 1
            if file.unwanted:
                self.unwanted += 1

def download(element, stream=True, query='', force=False):
    # Print details of element
    # print_element_details(element)
    # Extract data for CSV
    data = extract_element_data(element)
    cached = element.Releases
    print("Cached: ")
    print(cached)
    print(f"Number of cached releases found: {len(cached)}")  # Added print statement
    if query == '':
        query = element.deviation()
    wanted = [query]
    if not isinstance(element, releases.release):
        wanted = element.files()
    for release in cached[:]:
        # if release matches query
        if regex.match(query, release.title, regex.I) or force:
            if stream:
                release.size = 0
                for version in release.files:
                    if hasattr(version, 'files'):
                        if len(version.files) > 0 and version.wanted > len(wanted) / 2 or force:
                            cached_ids = []
                            for file in version.files:
                                cached_ids += [file.id]
                            # post magnet to real debrid
                            max_retries = 3
                            retry_delay = 5 # seconds
                            for attempt in range(max_retries):
                                try:
                                    response = post('https://api.real-debrid.com/rest/1.0/torrents/addMagnet', {'magnet': str(release.download[0])})
                                    if hasattr(response, 'error_code') and response.error_code == 25:
                                        retry = f'Error adding magnet due to ({response.error_code} service_unavailable)'
                                        print(f'[{str(datetime.datetime.now().strftime("%d/%m/%y %H:%M:%S"))}] [realdebrid] {retry}')
                                        continue
                                    else:
                                        if hasattr(response, 'id'):
                                            torrent_id = str(response.id)
                                        else:
                                            print(f'[{str(datetime.datetime.now().strftime("%d/%m/%y %H:%M:%S"))}] [realdebrid] error: No "id" in response for release: {release.title}')
                                            continue
                                except Exception as e:
                                    print(f'[{str(datetime.datetime.now().strftime("%d/%m/%y %H:%M:%S"))}] [realdebrid] error: could not add magnet for release. ({e})')
                                    if hasattr(e, 'response') and hasattr(e.response, 'status_code'):
                                        if e.response.status_code == 429:
                                            print(f'[{str(datetime.datetime.now().strftime("%d/%m/%y %H:%M:%S"))}] [realdebrid] Rate limit exceeded')
                                    continue
                            try:
                                response = post('https://api.real-debrid.com/rest/1.0/torrents/addMagnet', {'magnet': str(release.download[0])})
                                if hasattr(response, 'id'):
                                    torrent_id = str(response.id)
                                else:
                                    print(f'[{str(datetime.datetime.now().strftime("%d/%m/%y %H:%M:%S"))}] [realdebrid] error: No "id" in response for release: {release.title}')
                                    continue
                            except:
                                ui_print('[realdebrid] error: could not add magnet for release: ' + release.title, ui_settings.debug)
                                continue
                            response = post('https://api.real-debrid.com/rest/1.0/torrents/selectFiles/' + torrent_id, {'files': str(','.join(cached_ids))})
                            for attempt in range(max_retries):
                                response = get('https://api.real-debrid.com/rest/1.0/torrents/info/' + torrent_id)
                                actual_title = ""

                                if hasattr(response, 'links'):
                                    if len(response.links) == len(cached_ids):
                                        actual_title = response.filename
                                        release.download = response.links
                                    else:
                                        if response.status in ["queued", "magnet_conversion", "downloading", "uploading"]:
                                            if hasattr(element, "version"):
                                                debrid_uncached = True
                                                for i, rule in enumerate(element.version.rules):
                                                    if (rule[0] == "cache status") and (rule[1] == 'requirement' or rule[1] == 'preference') and (rule[2] == "cached"):
                                                        debrid_uncached = False
                                                if debrid_uncached:
                                                    import debrid as db
                                                    release.files = version.files
                                                    db.downloading += [element.query() + ' [' + element.version.name + ']']
                                                    ui_print('[realdebrid] adding uncached release: ' + release.title)
                                                    return True
                                        else:
                                            ui_print('[realdebrid] error: selecting this cached file combination returned a .rar archive - trying a different file combination.', ui_settings.debug)
                                            delete('https://api.real-debrid.com/rest/1.0/torrents/delete/' + torrent_id)
                                            continue
                                    if len(release.download) > 0:
                                        for link in release.download:
                                            try:
                                                response = post('https://api.real-debrid.com/rest/1.0/unrestrict/link', {'link': link})
                                            except Exception as e:
                                                print(f'[{str(datetime.datetime.now().strftime("%d/%m/%y %H:%M:%S"))}] Error: {e}')
                                                break
                                        release.files = version.files
                                        ui_print('[realdebrid] adding cached release: ' + release.title)
                                        # Write to CSV
                                        write_to_csv(data, release.title, actual_title)
                                        print("Writing to CSV" + CSV_FILE_PATH)
                                        if not actual_title == "":
                                            release.title = actual_title
                                        return True
                                elif response.error_code == 34:
                                    time.sleep(retry_delay)
                                    retry_delay += 1
                                    continue
                                else:
                                    print(f'[{str(datetime.datetime.now().strftime("%d/%m/%y %H:%M:%S"))}] [realdebrid] {response.error}')
                ui_print('[realdebrid] error: no streamable version could be selected for release: ' + release.title)
                return False
            else:
                try:
                    response = post('https://api.real-debrid.com/rest/1.0/torrents/addMagnet', {'magnet': release.download[0]})
                    time.sleep(0.1)
                    post('https://api.real-debrid.com/rest/1.0/torrents/selectFiles/' + str(response.id), {'files': 'all'})
                    ui_print('[realdebrid] adding uncached release: ' + release.title)
                    # Write to CSV
                    write_to_csv(data, release.title, actual_title)
                    print("Writing to CSV" + CSV_FILE_PATH)
                    return True
                except:
                    continue
        else:
            ui_print('[realdebrid] error: rejecting release: "' + release.title + '" because it doesn\'t match the allowed deviation', ui_settings.debug)
    return False
    write_processed_items(processed_items_file, new_processed_items)

# (required) Check Function
def check(element, force=False):
    if force:
        wanted = ['.*']
    else:
        wanted = element.files()
    unwanted = releases.sort.unwanted
    wanted_patterns = list(zip(wanted, [regex.compile(r'(' + key + ')', regex.IGNORECASE) for key in wanted]))
    unwanted_patterns = list(zip(unwanted, [regex.compile(r'(' + key + ')', regex.IGNORECASE) for key in unwanted]))

    hashes = []
    for release in element.Releases[:]:
        if len(release.hash) == 40:
            hashes += [release.hash]
        else:
            ui_print("[realdebrid] error (missing torrent hash): ignoring release '" + release.title + "' ", ui_settings.debug)
            element.Releases.remove(release)
    if len(hashes) > 0:
        response = get('https://api.real-debrid.com/rest/1.0/torrents/instantAvailability/' + '/'.join(hashes))
        ui_print("[realdebrid] checking and sorting all release files ...", ui_settings.debug)
        for release in element.Releases:
            release.files = []
            release_hash = release.hash.lower()
            if hasattr(response, release_hash):
                response_attr = getattr(response, release_hash)
                if hasattr(response_attr, 'rd'):
                    rd_attr = response_attr.rd
                    if len(rd_attr) > 0:
                        for cached_version in rd_attr:
                            version_files = []
                            for file_ in cached_version.__dict__:
                                file_attr = getattr(cached_version, file_)
                                debrid_file = file(file_, file_attr.filename, file_attr.filesize, wanted_patterns, unwanted_patterns)
                                version_files.append(debrid_file)
                            release.files += [version(version_files), ]
                        # select cached version that has the most needed, most wanted, least unwanted files and most files overall
                        release.files.sort(key=lambda x: len(x.files), reverse=True)
                        release.files.sort(key=lambda x: x.wanted, reverse=True)
                        release.files.sort(key=lambda x: x.unwanted, reverse=False)
                        release.wanted = release.files[0].wanted
                        release.unwanted = release.files[0].unwanted
                        release.size = release.files[0].size
                        release.cached += ['RD']
                        continue
        ui_print("done", ui_settings.debug)
