import pandas as pd
import json
import re
import boto3
from datetime import datetime


from collections.abc import Mapping
from typing import Any, Optional, List
import urllib.parse
import re


_DOMAIN_KEY_MAP = {'google.com':'q', 'yahoo.com': 'p', 'bing.com':'q', 'esshopzilla.com': 'k'}

from dataclasses import dataclass

# A node which holds the webpage hit data.
@dataclass
class WebpageNode:
  """Class keeping track of webpages as Nodes."""
  realized_revenue: float
  url: str
  next_nodes: Optional[List[Any]] = None

  def add_next(self, node) -> float:
    if not self.next_nodes:
      self.next_nodes = []
    self.next_nodes.append(node)

# A crawl graph for each browse session.
@dataclass
class CrawlGraph:
  """Class keeping track of webpages as Nodes."""
  ip: str
  referrer: str
  root_node: WebpageNode
  date_time: str

  def _recursively_crawl_for_revenue(self, node, data):
    data['Revenue'] += node.realized_revenue
    if not node.next_nodes:
      return
    for child_node in node.next_nodes:
      self._recursively_crawl_for_revenue(child_node, data)

  # Perform a dfs on the root node for each crawl graph.
  def crawl_for_revenue(self) -> float:
    data = {'Revenue':0.0}
    self._recursively_crawl_for_revenue(self.root_node, data)
    return data['Revenue']


class DataExtractor():
  def extract_domain_key(self, referer):
    domain = '.'.join(re.search("(?<=://)(?:www\.)?([A-Za-z0-9\-\.]+\.[A-Za-z]{2,3})", referer).group(1).split('.')[-2:])
    search_key = ''
    if domain:
      kw_match = re.search(_DOMAIN_KEY_MAP[domain] + '=([^\&\/]+)',
                                          referer)
      if kw_match:
        search_key = kw_match.group(1)
      search_key = search_key.replace('+', ' ').lower()
    return domain, search_key

  def extract_revenue(self, row: Mapping[str, str], product_list_key = 'product_list', event_list_key='event_list'):
    if not row[product_list_key]:
      return 0.0
    
    product_list_length = row.get(product_list_key, '')
    if len(product_list_length.split(';')) < 4:
        return 0.0
    revenue = row.get(row[product_list_key].split(';')[3], 0.0)
    if isinstance(row[event_list_key], float):
      event_list = int(row[event_list_key])
    else:
      event_list = row[event_list_key]
    events = str(event_list).split(',')
    if '1' not in set(events):
      return 0.0
    return float(revenue)
  
  def timestamp_value(self,date_time ):

    date_obj = datetime.strptime(date_time, '%m/%d/%y %H:%M')
    formatted_date = date_obj.strftime('%Y-%m-%d')

    print(formatted_date)
    return formatted_date

def get_visit_hash(ip, url, delim='||'):
  return delim.join([ip, url])


def extract_revenue_data_from_tsv(data):
  #data = pd.read_csv(tsv_file, sep='\t')
  data = data.fillna('')
  extractor = DataExtractor()
  crawl_graphs = []
  node_data = dict()

  # Build the graphs for each browse session.
  for i, row in data.iterrows():
    node_data[get_visit_hash(row['ip'], row['page_url'])] = WebpageNode(realized_revenue=extractor.extract_revenue(row), url=row['page_url'])

    # Keep in mind that when a node is iterated over, its referrer node has already been visited as the dtaframe is chronological.
    if get_visit_hash(row['ip'], row['referrer']) in node_data:
      node_data[get_visit_hash(row['ip'], row['referrer'])].add_next(node_data[get_visit_hash(row['ip'], row['page_url'])])
    else:
      crawl_graphs.append(CrawlGraph(row['ip'], row['referrer'], node_data[get_visit_hash(row['ip'], row['page_url'])], row['date_time']))

  # Extract revenue domain and search key from each browse session
  df = pd.DataFrame(columns=['Search Engine Domain', 'Referrer', 'Search Keyword', 'IP','Revenue', 'date'])
  dates =set()
  for crawl_graph in crawl_graphs:
    r = crawl_graph.crawl_for_revenue()
    d,k = extractor.extract_domain_key(crawl_graph.referrer)
    t= extractor.timestamp_value(crawl_graph.date_time)
    df = df.append({'Search Engine Domain': d,'Referrer': crawl_graph.referrer,'IP': crawl_graph.ip, 'Search Keyword': k, 'Revenue': r, 'date':t}, ignore_index=True)
    # t= extractor.timestamp_value(crawl_graph.date_time)
    dates.add(extractor.timestamp_value(crawl_graph.date_time))
  return df,dates

