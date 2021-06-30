##!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, requests, os, calendar, json
from datetime import datetime, timedelta

# Initial setup
# sigsci
api_host = 'https://dashboard.signalsciences.net'
email = os.environ.get('SIGSCI_EMAIL')
token = os.environ.get('SIGSCI_TOKEN')
corp_name = 'mycorp'
site_name = os.environ.get('SIGSCI_SITE')
# ELK
elk_host = os.environ.get('ELK_HOST')
elk_url = "https://"+elk_host+"/_bulk"
elk_index = "waf"
elk_type  = "sigsci"
# logs time interval in minutes
time_delta = 5


# functions
def get_sig_sci_logs(url):
    # Loop across all the data and output it in one big JSON object
    headers = {
        'Content-type': 'application/json',
        'x-api-user': email,
        'x-api-token': token
    }
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print ('SigSci cnx : Unexpected status: %s response: %s' % (response.status_code, response.text))
        return

    return response


def format_to_elk_bulk_data(response):
    if not response.get('data'):
        print ("No logs from SigSci for corp %s and site %s (last %s min): exiting" % (corp_name, site_name, time_delta))
        return

    elk_data = ''
    first = True
    for request in response.get('data'):
        # adapt to default ELK timestamp name field
        request['@timestamp']=request.pop('timestamp')
        if first:
            first = False
        else:
            elk_data += '\n'

        # adding elk index for bulk insert
        elk_data += '{"index": {"_index": "%s", "_type": "%s", "_id": "%s"}}\n' % (elk_index, elk_type, request['id'])
        
        # pretty formating headers
        headers_in = request.pop("headersIn")
        for elem in headers_in:
            request['client.header.'+elem[0]] = elem[1]
        headers_out = request.pop("headersOut")
        if headers_out:
            for elem in headers_out:
                request['server.header.'+elem[0]] = elem[1]
        tags = request.pop("tags")
        for elem in tags:
            tags_line = 'redaction: ' + elem['redaction'] +'; '
            tags_line += 'value: ' + elem['value'] +'; '
            tags_line += 'link: ' + elem['link'] +'; '
            tags_line += 'location: ' + elem['location'] +'; '
            tags_line += 'detector: ' + elem['detector'] +'; '
            tags_line += 'type: ' + elem['type']
            request['tags.'+ elem['type']] = tags_line

        data = json.dumps(request)
        elk_data += data
    elk_data += '\n'
    return elk_data


def send_to_elk(elk_data):
    r = requests.put(
        url  = elk_url,
        data = elk_data,
        headers = { 'Content-type': 'application/json'}
        )
    if r.status_code != 200:
        print ('ELK cnx : Unexpected status: %s \nResponse: \n%s' % (r.status_code, r.text))


# Calculate UTC timestamps 
until_time = datetime.utcnow().replace(second=0, microsecond=0)
# sigsci resctriction: The until parameter has a maximum of five minutes in the past
until_time = until_time - timedelta(minutes=5)
from_time = until_time - timedelta(minutes=time_delta)
until_time = calendar.timegm(until_time.utctimetuple())
from_time = calendar.timegm(from_time.utctimetuple())

url = api_host + ('/api/v0/corps/%s/sites/%s/feed/requests?from=%s&until=%s' % (corp_name, site_name, from_time, until_time))
while True:
    response_raw = get_sig_sci_logs(url)
    if not response_raw:
        break

    response = json.loads(response_raw.text)

    elk_data = format_to_elk_bulk_data(response)
    if not elk_data:
        break

    send_to_elk(elk_data)

    # endpoint returns 1,000 requests at a time
    next_url = response['next']['uri']
    if next_url == '':
        break
    url = api_host + next_url
