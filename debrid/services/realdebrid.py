import asyncio
import aiohttp
import regex
import datetime
import time
from requests import get, post
from some_module import ui_print, ui_settings, file, version, write_to_csv, write_processed_items  # Assuming these are imported from somewhere

async def fetch(session, url, retries=2):
    for _ in range(retries):
        try:
            async with session.get(url, timeout=30) as response:
                return await response.json()
        except asyncio.TimeoutError:
            continue
    return None

async def fetch_all(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url) for url in urls]
        return await asyncio.gather(*tasks)

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
            hashes.append(release.hash)
        else:
            ui_print("[realdebrid] error (missing torrent hash): ignoring release '" + release.title + "' ", ui_settings.debug)
            element.Releases.remove(release)

    if len(hashes) > 0:
        loop = asyncio.get_event_loop()
        urls = ['https://api.real-debrid.com/rest/1.0/torrents/instantAvailability/' + '/'.join(hashes[i:i+20]) for i in range(0, len(hashes), 20)]
        responses = loop.run_until_complete(fetch_all(urls))

        ui_print("[realdebrid] checking and sorting all release files ...", ui_settings.debug)
        
        cached_count = 0  # Initialize cached count
        
        for release in element.Releases:
            release.files = []
            release_hash = release.hash.lower()
            response = next((resp for resp in responses if resp and release_hash in resp), None)
            if response and release_hash in response:
                response_attr = response[release_hash]
                if 'rd' in response_attr:
                    rd_attr = response_attr['rd']
                    if len(rd_attr) > 0:
                        for cached_version in rd_attr:
                            version_files = []
                            for file_ in cached_version:
                                file_attr = cached_version[file_]
                                debrid_file = file(file_, file_attr['filename'], file_attr['filesize'], wanted_patterns, unwanted_patterns)
                                version_files.append(debrid_file)
                            release.files.append(version(version_files))
                        # select cached version that has the most needed, most wanted, least unwanted files and most files overall
                        release.files.sort(key=lambda x: len(x.files), reverse=True)
                        release.files.sort(key=lambda x: x.wanted, reverse=True)
                        release.files.sort(key=lambda x: x.unwanted, reverse=False)
                        release.wanted = release.files[0].wanted
                        release.unwanted = release.files[0].unwanted
                        release.size = release.files[0].size
                        release.cached.append('RD')
                        cached_count += 1  # Increment cached count
                        continue
        
        ui_print(f"done. {cached_count} out of {len(element.Releases)} releases are cached.", ui_settings.debug)

def process_release(release, stream, force, query, wanted):
    if regex.match(query, release.title, regex.I) or force:
        if stream:
            release.size = 0
            for version in release.files:
                if hasattr(version, 'files'):
                    if len(version.files) > 0 and version.wanted > len(wanted) / 2 or force:
                        cached_ids = []
                        for file in version.files:
                            cached_ids.append(file.id)
                        # post magnet to real debrid
                        max_retries = 3
                        retry_delay = 5  # seconds
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
                                                db.downloading.append(element.query() + ' [' + element.version.name + ']')
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
                                    if actual_title != "":
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
               
