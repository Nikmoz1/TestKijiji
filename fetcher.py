import math
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import dateparser
from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine

meta = MetaData()
kijiji_elements = Table(
    'kijiji_elements', meta,
    Column('id', Integer, primary_key=True),
    Column('title', String),
    Column('description', String),
    Column('location', String),
    Column('image', String),
    Column('date', String),
    Column('beds', String),
    Column('currency', String),
    Column('price', String),
)
engine = create_engine('postgresql+psycopg2://postgres:postgres@db:5432/postgres')
meta.create_all(engine)


async def get_page_data(session, page):
    print(f"start {page}")
    url = f"https://www.kijiji.ca/b-apartments-condos/city-of-toronto/page-{page}/c37l1700273"
    async with session.get(url=url) as response:
        response_text = await response.text()
        soup = BeautifulSoup(response_text, "html.parser")
        items = soup.find_all("div", class_="search-item")
        kijiji_list = []
        for i in items:
            try:
                image = i.select_one("img")["data-src"].strip()
            except KeyError:
                image = None
            kijiji_elements_add = {
                "title": i.find("a", class_="title").text,
                "description": i.select_one(".description").text.strip().replace("\n", "").replace("  ", ""),
                "location": i.select_one(".location > span").text.strip(),
                "image": image,
                "date": dateparser.parse(i.select_one(".date-posted").text.strip()).strftime("%d-%m-%Y"),
                "beds": i.select_one(".bedrooms").text.strip().split(":")[1].replace("\n", "").replace("  ", ""),
                "currency": i.select_one(".price").text.strip()[0].strip()[0],
                "price": i.select_one(".price").text.strip()[1:],
            }
            kijiji_list.append(kijiji_elements_add)

        return kijiji_list


async def gather_data():
    print("start gather_data")
    url = "https://www.kijiji.ca/b-apartments-condos/city-of-toronto/c37l1700273"
    headers = {
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 "
                      "Safari/537.36",
        "cookie": "MUID=22FCA686B9FD64150D74B498B8EF6561"}
    async with aiohttp.ClientSession() as session:
        response = await session.get(url=url, headers=headers)
        soup = BeautifulSoup(await response.text(), "lxml")
        page_string = soup.find("span", class_="resultsShowingCount-1707762110").text
        page_count = math.ceil(int(page_string[page_string.find("of") + 2: page_string.find("results")]) / 40)
        tasks = []
        for page in range(1, page_count):
            task = asyncio.create_task(get_page_data(session, page))
            tasks.append(task)
        all_ads = await asyncio.gather(*tasks)
        all_formatted_ads = [x for l in all_ads for x in l]
        with engine.connect() as conn:
            conn.execute(kijiji_elements.insert().values(all_formatted_ads))


def main():
    asyncio.run(gather_data())


if __name__ == "__main__":
    main()
