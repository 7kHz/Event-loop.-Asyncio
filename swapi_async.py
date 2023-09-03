import datetime
import aiohttp
import asyncio
from more_itertools import chunked
from models import Session, Base, SwapiPeople, engine

CHUNK_SIZE = 1


async def get_people(client, people_id):
    response = await client.get(f'https://swapi.dev/api/people/{people_id}')
    json_data = await response.json()
    return json_data


async def get_data(client, url):
    response = await client.get(url)
    data = await response.json()
    return data


async def get_inner_data(client, results, field):
    data = []
    for item in results:
        if len(item) > 1:
            if field == 'homeworld':
                data.append(get_data(client, item.get('homeworld')))
            else:
                if len(item.get(field)) != 0:
                    for url in item.get(field):
                        data.append(get_data(client, url))
    res = await asyncio.gather(*data)
    data.clear()
    return res


async def insert_to_db(results, films, homeworld, species, starships, vehicles):
    async with Session() as session:
        swapi_people_list = [SwapiPeople(birth_year=item.get('birth_year'),
                                         eye_color=item.get('eye_color'),
                                         films=', '.join([film.get('title') for film in films]),
                                         gender=item.get('gender'),
                                         hair_color=item.get('hair_color'),
                                         height=item.get('height'),
                                         homeworld=', '.join([home.get('name') for home in homeworld]),
                                         mass=item.get('mass'),
                                         name=item.get('name'),
                                         skin_color=item.get('skin_color'),
                                         species=', '.join([specie.get('name')
                                                            for specie in species], ),
                                         starships=', '.join([ship.get('name') for ship in starships]),
                                         vehicles=', '.join([vehicle.get('name') for vehicle in vehicles])
                                         ) for item in results if len(item) > 1]
        session.add_all(swapi_people_list)
        await session.commit()


async def main():
    fields = ['films', 'homeworld', 'species', 'starships', 'vehicles']
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    async with aiohttp.ClientSession() as client:
        for ids_chunk in chunked(range(100), CHUNK_SIZE):
            print(ids_chunk, end=' ')
            coros = [get_people(client, i) for i in ids_chunk]
            results = await asyncio.gather(*coros)
            films = await get_inner_data(client, results, fields[0])
            homeworld = await get_inner_data(client, results, fields[1])
            species = await get_inner_data(client, results, fields[2])
            starships = await get_inner_data(client, results, fields[3])
            vehicles = await get_inner_data(client, results, fields[4])
            insert_to_db_coro = insert_to_db(results, films, homeworld, species, starships, vehicles)
            asyncio.create_task(insert_to_db_coro)
    current_task = asyncio.current_task()
    tasks_to_await = asyncio.all_tasks() - {current_task, }
    for task in tasks_to_await:
        await task


if __name__ == '__main__':
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)
