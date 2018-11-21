# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
Utilities for parsing Tap and Gaia TapPlus HTML and XML responses
"""

import re
from collections import namedtuple
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from astropy.table import Table

__all__ = [
    'parse_html_response_error'
]


def parse_html_response_error(html):
    """Return a useful message from failed TAP request"""
    soup = BeautifulSoup(html, 'html.parser')
    # this is not robust at all....
    message_li = soup.find(string=re.compile('Message')).parent.parent
    return message_li.text

    
# Unique name spaces in all xml files in tests/data
# xmlns="http://www.ivoa.net/xml/VOTable/v1.3">
# xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0"
# xmlns:xlink="http://www.w3.org/1999/xlink"
# xmlns:xs="http://www.w3.org/2001/XMLSchema"
# xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.ivoa.net/xml/UWS/v1.0 http://www.ivoa.net/xml/UWS/v1.0"  total="1">
# xmlns:vod="http://www.ivoa.net/xml/VODataService/v1.1" xsi:type="vod:TableSet"
# xmlns:esatapplus="http://esa.int/xml/EsaTapPlus" xsi:schemaLocation="http://www.ivoa.net/xml/VODataService/v1.1 http://www.ivoa.net/xml/VODataService/v1.1 http://esa.int/xml/EsaTapPlus http://gea.esac.esa.int/tap-server/xml/esaTapPlusAttributes.xsd">

ns = {'uws': "http://www.ivoa.net/xml/UWS/v1.0",
      'xlink': "http://www.w3.org/1999/xlink"}

items = [
    "uws:jobId",
    "uws:runId",
    "uws:ownerId",
    "uws:phase",
    "uws:quote",
    "uws:startTime",
    "uws:endTime",
    "uws:executionDuration",
    "uws:destruction",
    "uws:creationTime",
    "uws:locationId",
    "uws:name"]

lookup = {
        "jobid":            "uws:jobId",
        "runid":            "uws:runId",
        "ownerid":          "uws:ownerId",
        "phase":            "uws:phase",
        "quote":            "uws:quote",
        "starttime":        "uws:startTime",
        "endtime":          "uws:endTime",
        "executionduration":"uws:executionDuration",
        "destruction":      "uws:destruction",
        "creationtime":     "uws:creationTime",
        "locationid":       "uws:locationId",
        "name":             "uws:name",
}

root = ET.parse('tests/data/test_tables.xml').getroot()

# root is tableset
def parse_tableset(xml):
    root = ET.fromstring(xml)
    # save (schema, table name, table description) for each table
    rows = []
    tables = []
    for schema in root.findall('.//schema'):
        schema_name = schema.find('name').text
        # schema_desc = schema.find('description').text
        if schema_name in ['tap_schema', 'external']:
            continue
        for table in schema.findall('.//table'):
            table_name = table.find('name').text
            table_desc = table.find('description').text
            rows.append((schema_name, table_name, table_desc))
            columns = []
            for col in table.findall('.//column'):
                # columns has these tags
                # {('name', 'description', 'unit', 'dataType'),
                # ('name', 'description', 'unit', 'dataType', 'flag', 'flag'),
                # ('name', 'description', 'unit', 'ucd', 'utype', 'dataType'),
                # ('name', 'description', 'unit', 'ucd', 'utype', 'dataType', 'flag', 'flag')}
                col_name = col.find('name').text
                col_desc = col.find('description').text
                col_unit = col.find('unit').text
                col_dtype = col.find('dataType').text
                columns.append((col_name, col_desc, col_unit, col_dtype))
            tables.append(Table(rows=columns, names=['name', 'description', 'unit', 'dtype']))
    tablelist = Table(rows=rows, names=['schema', 'name', 'description'])
    return tablelist, tables


class ColumnMeta(
    namedtuple('ColumnMeta', ['name', 'unit', 'datatype', 'description'],
    defaults=['', '', '', ''])):

    def __repr__(self):
        s = 'Column(name="{s.name:s}", unit="{s.unit:s}", description='.format(s=self)
        remaining_length = 80-len(s)
        s += '"{desc}"'.format(desc=self.description[:remaining_length])
        s += "..." if len(self.description) > remaining_length else "" + ")"
        return s
    
    def __str__(self):
        return "Column name: {s.name}\nunit: {s.unit}\ndesription: {s.description}".format(s=self)


class TableMeta(
    namedtuple('TableMeta', ['name', 'schema', 'description', 'columns'])):

    @property
    def as_table(self):
        rows = list(map(lambda c: (c.name, c.datatype, c.unit, c.description[:60]), self.columns)) 
        return Table(rows=rows, names=['name', 'datatype', 'unit', 'short_description'])
    
    def __repr__(self):
        return "{name:s}, {ncolumns:d} columns".format(
                   name=self.name, ncolumns=len(self.columns))


def xstr(s):
    return '' if s is None else str(s)
        

def parse_tableset2(xml):
    root = ET.fromstring(xml)
    # save (schema, table name, table description) for each table
    tables = []
    for schema in root.findall('.//schema'):
        schema_name = schema.find('name').text
        # schema_desc = schema.find('description').text
        if schema_name in ['tap_schema', 'external']:
            continue
        for table in schema.findall('.//table'):
            table_name = table.find('name').text
            table_desc = table.find('description').text
            table_desc = '' if table_desc is None else table_desc.strip()
            columns = []
            for col in table.findall('.//column'):
                # columns has these tags
                # {('name', 'description', 'unit', 'dataType'),
                # ('name', 'description', 'unit', 'dataType', 'flag', 'flag'),
                # ('name', 'description', 'unit', 'ucd', 'utype', 'dataType'),
                # ('name', 'description', 'unit', 'ucd', 'utype', 'dataType', 'flag', 'flag')}
                col_name = xstr(col.find('name').text).strip()
                col_desc = xstr(col.find('description').text).strip()
                col_unit = xstr(col.find('unit').text).strip()
                col_dtype = xstr(col.find('dataType').text).strip()
                columns.append(ColumnMeta(col_name, col_unit, col_dtype, col_desc))
            tables.append(
                TableMeta(table_name, schema_name, table_desc, columns))
    return tables


class Job(object):
    def __init__(self):
        pass


def test_parse_job():
    tree = ET.parse('top5gaia_async.xml')
    root = tree.getroot()

    job = Job()
    for k, v in lookup.items():
        print(k)
        setattr(job, k, root.find(v, ns).text)

    result_urls = [r.attrib['{{{xlink}}}href'.format(**ns)]
            for r in root.findall('.//uws:results//uws:result', ns)]

