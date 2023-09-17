# -*- coding: utf-8 -*-

from urllib.parse import urlparse, parse_qs, urlencode, urljoin
from io import BytesIO
from hashlib import sha256
import requests
from bs4 import BeautifulSoup
from robotexclusionrulesparser import RobotExclusionRulesParser
import PyPDF2


USER_AGENT = 'aword-bot-0.1'


def normalize_url(url):
    parsed_url = urlparse(url)
    sorted_query = sorted(parse_qs(parsed_url.query).items(), key=lambda x: x[0])
    irrelevant_params = {'utm_source', 'utm_medium', 'utm_campaign'}
    filtered_query = [(k, v) for k, v in sorted_query if k not in irrelevant_params]
    normalized_url = parsed_url._replace(query=urlencode(filtered_query, doseq=True)).geturl()
    return normalized_url.rstrip('/')


def hash_content(content):
    return sha256(content.encode('utf-8')).hexdigest()


def get_all_links(soup, base_url):
    for link in soup.find_all(['a', 'link'], href=True):
        print(urljoin(base_url, link['href']))
        yield urljoin(base_url, link['href'])


def extract_text_from_soup(soup):
    return soup.get_text()


def extract_text_from_pdf(content):
    with BytesIO(content) as pdf_file:
        reader = PyPDF2.PdfFileReader(pdf_file)
        text = "\n".join(reader.getPage(i).extractText() for i in range(reader.numPages))
    return text


def can_fetch(robots_parser, url, user_agent):
    return robots_parser.is_allowed(user_agent, url)


def fetch_and_parse(url):
    headers = {
        "User-Agent": USER_AGENT
    }
    response = requests.get(url, headers=headers, timeout=10)
    content_type = response.headers.get("Content-Type", "").split(";")[0]

    if "text/html" in content_type:
        soup = BeautifulSoup(response.content, 'html.parser')
        return "html", soup
    if "application/pdf" in content_type:
        return "pdf", response.content
    return "unknown", None


def main(start_url, depth=2, user_agent="aweb-bot-0.1"):
    robots_url = urljoin(start_url, "/robots.txt")
    robots_parser = RobotExclusionRulesParser()
    try:
        robots_parser.fetch(robots_url)
    except requests.RequestException as e:
        print(f"Error fetching robots.txt from {robots_url}: {e}")

    visited_urls = set()
    urls_to_visit = {start_url}
    content_hashes = set()

    for _ in range(depth):
        new_urls = set()
        for url in urls_to_visit:
            normalized_url = normalize_url(url)
            if normalized_url not in visited_urls and can_fetch(robots_parser,
                                                                normalized_url,
                                                                user_agent):
                visited_urls.add(normalized_url)
                try:
                    content_type, content = fetch_and_parse(normalized_url)
                    print(content)

                    if content_type == "html":
                        page_text = extract_text_from_soup(content)
                        new_urls.update(get_all_links(content, normalized_url))
                    elif content_type == "pdf":
                        page_text = extract_text_from_pdf(content)
                    else:
                        print(f"Unsupported content type for {normalized_url}: {content_type}")
                        continue

                    content_hash = hash_content(page_text)
                    if content_hash in content_hashes:
                        print(f"Duplicate content detected for {normalized_url}")
                        continue

                    content_hashes.add(content_hash)
                    with open(f"{hash(normalized_url)}.txt", "w", encoding="utf-8") as f:
                        f.write(page_text)

                except Exception as e:
                    print(f"Error fetching {normalized_url}: {e}")

        urls_to_visit = new_urls


if __name__ == '__main__':
    main("https://greaterskies.com/")
