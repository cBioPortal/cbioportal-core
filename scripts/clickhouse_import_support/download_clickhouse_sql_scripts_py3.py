#!/usr/bin/env python3

from urllib.parse import urlparse
import argparse
import base64
import http.client
import json
import os
import sys

def create_arg_parser():
    usage = "usage: %prog destination_directory github_branch_name"
    parser = argparse.ArgumentParser(
                    prog='download_clickhouse_sql_scripts_py3.py',
                    description='Downloads all files ending with \'.sql\' in the /src/main/resources/db_scripts/clickhouse directory of the cBioPortal github repository.')
    parser.add_argument('destination_directory', help = "local filesystem directory where downloads will be stored")
    parser.add_argument('--github_branch_name', default = "master")
    return parser

def exit_if_args_are_invalid(args):
    if not os.path.isdir(args.destination_directory):
        sys.exit("destination_directory argument (" + args.destination_directory + ") does not exist or is not a directory")

def download_files_from_github(args):
    GITHUB_HOST_NAME = "api.github.com"
    GITHUB_API_PATH = "/repos/cBioPortal/cbioportal/contents/src/main/resources/db-scripts/clickhouse"
    github_query_string = f'ref={args.github_branch_name}'
    github_user_agent = f'python http.client(v{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro})'
    github_headers = {"Host" : GITHUB_HOST_NAME,
            "User-Agent" : github_user_agent,
            "Accept" : "application/vnd.github+json",
            "X-GitHub-Api-Version" : "2022-11-28"}
    github_conn = http.client.HTTPSConnection(GITHUB_HOST_NAME, timeout=16)
    github_request_string = f'{GITHUB_API_PATH}?{github_query_string}'
    github_conn.request("GET", github_request_string, headers = github_headers)
    directory_content_response = github_conn.getresponse()
    directory_content = json.loads(directory_content_response.read().decode("utf-8"))
    files_to_be_downloaded = [x for x in directory_content if os.path.basename(urlparse(x["url"]).path).casefold().endswith(".sql")]
    destination_directory_path = os.path.normpath(args.destination_directory)
    for github_file in files_to_be_downloaded:
        filename = os.path.basename(urlparse(github_file["url"]).path)
        print(f'downloading {github_file["url"]} for file {filename}')
        github_conn.request("GET", github_file["url"], headers = github_headers)
        file_content_response = json.loads(github_conn.getresponse().read().decode("utf-8"))
        file_data = base64.b64decode(file_content_response["content"]).decode()
        output_file_path = os.path.join(destination_directory_path, filename)
        with open(output_file_path, "w") as output_file:
            output_file.write(file_data)
    
def main():
    parser = create_arg_parser()
    args = parser.parse_args()
    exit_if_args_are_invalid(args)
    download_files_from_github(args)

if __name__ == '__main__':
    main()
