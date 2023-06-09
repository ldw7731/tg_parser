import datetime, configparser, os.path, asyncio, asyncpg
from sqlalchemy import MetaData, Table, Column, BIGINT, SMALLINT, BOOLEAN, String, TIMESTAMP, LargeBinary, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.dialects.postgresql import insert
from contextlib import closing
from telethon import functions
from telethon.tl.types import PeerChannel
from telethon.sync import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.messages import GetHistoryRequest


class AsyncIteratorWrapper():
    def __init__(self, obj):
        self._it = iter(obj)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            value = next(self._it)
        except StopIteration:
            raise StopAsyncIteration
        return value


class tg_client():
    def __init__(self):
        self.api_id = None
        self.api_hash = None
        self.phone = None
        self.username = None
        self.session_token = None

    async def get_config(self):
        try:
            tg_config = await loop.run_in_executor(None, get_config, 'tg_config.ini')
        except Exception as ex:
            tg_config = None
            print('Ошибка получения настроек telegram: ' + str(type(ex)))
        if tg_config is not None:
            try:
                self.api_id = tg_config['Telegram']['api_id']
                self.api_hash = tg_config['Telegram']['api_hash']
                self.phone = tg_config['Telegram']['phone']
                self.username = tg_config['Telegram']['username']
                self.session_token = tg_config['Telegram']['phone'] + '.session'
            except Exception as ex:
                print('Ошибка получения настроек telegram: ' + str(type(ex)))

    async def start(self):
        client = None
        if self.session_token is not None and self.api_id is not None and self.api_hash is not None:
            if os.path.exists(self.session_token):
                try:
                    client = TelegramClient(self.session_token, self.api_id, self.api_hash)
                except Exception as ex:
                    print('Не удалось подключиться к TelegramClient через токен сессии. Тип ошибки: {}'.format(
                        str(type(ex))))
            else:
                try:
                    client = TelegramClient(self.phone, self.api_id, self.api_hash)
                except Exception as ex:
                    print('Не удалось подключиться к TelegramClient по номеру телефона. Тип ошибки: {}'.format(
                        str(type(ex))))
        else:
            print('Не заполнены параметры подключения telegram')
        if client is not None:
            await client.start()
            if not await client.is_user_authorized():
                await client.send_code_request(self.phone)
                try:
                    await client.sign_in(self.phone, input('Enter the code: '))
                except SessionPasswordNeededError:
                    await client.sign_in(password=input('Password: '))
        return client


class pg_engine():
    def __init__(self):
        self.db_type = None
        self.db_user = None
        self.db_pass = None
        self.db_host = None
        self.db_port = None
        self.db_name = None

    async def get_config(self):
        try:
            db_config = await loop.run_in_executor(None, get_config, 'db_config.ini')
        except Exception as ex:
            db_config = None
            print('Ошибка получения настроек database: ' + str(type(ex)))
        if db_config is not None:
            try:
                self.db_type = db_config['database']['db_type']
                self.db_user = db_config['database']['db_user']
                self.db_pass = db_config['database']['db_pass']
                self.db_host = db_config['database']['db_host']
                self.db_port = db_config['database']['db_port']
                self.db_name = db_config['database']['db_name']
            except Exception as ex:
                print('Ошибка получения настроек database: ' + str(type(ex)))

    async def create_sqlengine(self):
        self.sqlengine = None
        try:
            str_connect = '{}+asyncpg://{}:{}@{}:{}/{}'.format(
                self.db_type,
                self.db_user,
                self.db_pass,
                self.db_host,
                self.db_port,
                self.db_name
            )
            self.sqlengine = create_async_engine(str_connect, echo=False)
        except Exception as ex:
            print('Не удалось подключиться к БД {}. Тип ошибки: {}'.format(self.db_name, str(type(ex))))

    async def fill_db_obj(self, dict_obj, tg_o_dict, index_dict):
        if tg_o_dict["_"] == "Channel":
            #  указали индексное поле
            index_dict['index_col'] = "group_id"
            for col in tg_o_dict.keys():
                # особым образом заполняются индексные поля для группы и фото
                # id - не хотим хранить с таким названием, а фото может быть вложенным для группы
                if col.lower() == "id":
                    dict_obj["group_id"] = tg_o_dict["id"]
                elif col.lower() == "photo":
                    if "photo_id" in tg_o_dict['photo']:
                        dict_obj["photo"] = tg_o_dict['photo']['photo_id']
                    else:
                        dict_obj["photo"] = None
                else:
                    if col.lower() in dict_obj:
                        dict_obj[col.lower()] = tg_o_dict[col.lower()]

        elif tg_o_dict["_"] == "Message":
            #  указали индексное поле
            index_dict['index_col'] = "msg_id"
            for col in tg_o_dict.keys():
                if col.lower() == "id":
                    dict_obj["msg_id"] = tg_o_dict["id"]
                else:
                    if col.lower() in dict_obj:
                        dict_obj[col.lower()] = tg_o_dict[col.lower()]
        elif tg_o_dict["_"] == "Photo" or tg_o_dict["_"] == "ChatPhoto":
            # проверим есть ли чтото в полях, где хранятся картинки (другие медиа файлы я не рассматриваю в данном проекте)
            st_filled = True
            fr_filled = True
            if 'stripped_thumb' in tg_o_dict.keys():  # ключ есть в объекте
                if str(tg_o_dict["stripped_thumb"]) == 'None':  # значение заполнено
                    st_filled = False
            if 'file_reference' in tg_o_dict.keys():  # ключ есть в объекте
                if str(tg_o_dict["file_reference"]) == 'None':  # значение заполнено
                    fr_filled = False
            if st_filled or fr_filled:  # проверяем
                for col in tg_o_dict.keys():
                    if col.lower() == "id":
                        dict_obj["photo_id"] = tg_o_dict["id"]
                    elif col.lower() in dict_obj:
                        dict_obj[col.lower()] = tg_o_dict[col.lower()]

        return dict_obj

    async def insert_tg_object(self, tg_meta, tg_object, index_dict, connection):
        # выгрузим тг-объект в словарь
        tg_o_dict = tg_object.to_dict()

        # создадим пустой словарь с колонками которые будут храниться в базе
        empty_obj = dict.fromkeys([col.name for col in tg_meta.columns._all_columns])
        is_empty = empty_obj.copy()
        # заполним полученный словарь значениями из тг-объекта
        db_obj = await self.fill_db_obj(empty_obj, tg_o_dict, index_dict)

        # тепреь если словарь не пустой выполним вставку
        if db_obj != is_empty:
            try:
                db_insert = insert(tg_meta).values(db_obj)
                insert_table_sql = db_insert.on_conflict_do_nothing(index_elements=[index_dict['index_col']])
                await connection.execute(insert_table_sql)
            finally:
                connection.close()

    async def add_group_to_db(self, chat):
        if self.sqlengine is not None:
            meta = MetaData()
            async with self.sqlengine.connect() as conn:

                tg_photos = get_tg_photos(meta)
                if chat.photo is not None:
                    await self.insert_tg_object(tg_photos, chat.photo, {'index_col': 'photo_id'}, conn)

                tg_groups = get_tg_group(meta)
                if chat is not None:
                    await self.insert_tg_object(tg_groups, chat, {'index_col': 'id'}, conn)

                if not conn.closed:
                    await conn.commit()
                print("new group add to db: " + str(chat))

    async def add_msg_to_db(self, msg):
        if self.sqlengine is not None:
            meta = MetaData()
            async with self.sqlengine.connect() as conn:

                tg_photos = get_tg_photos(meta)
                if msg.photo is not None:
                    await self.insert_tg_object(tg_photos, msg.photo, {'index_col': 'photo_id'}, conn)

                tg_msgs = get_tg_msgs(meta)
                if msg is not None:
                    await self.insert_tg_object(tg_msgs, msg, {'index_col': 'id'}, conn)

                if not conn.closed:
                    await conn.commit()
                    print("new msg add to db: " + str(msg))


def get_config(file_name):
    try:
        config = configparser.ConfigParser()
        config.read(file_name)
        return config
    except Exception as ex:
        print('Не удалось считать файл {}. Тип ошибки: {}'.format(file_name, str(type(ex))))
        return None


async def get_added_chats_id(conn):
    query = "select group_id from tg_groups;"
    result = await conn.execute(text(query))
    list_of_dict = result.mappings().all()
    added_chats = [d['group_id'] for d in list_of_dict]
    return added_chats


def read_themes_file(file_name):
    with open(file_name) as f:
        res = f.read().split('\n')
        if not isinstance(res, list):
            print('Результат чтения файла {} не является массивом!'.format(file_name))
            return None
        else:
            if len(res) == 0:
                print('Файла {} пуст!'.format(file_name))
                return None
            else:
                return res


def get_tg_photos(meta):
    TgGroup = Table(
        'tg_photos', meta,
        Column('photo_id', BIGINT, primary_key=True, nullable=False),
        Column('dc_id', SMALLINT),
        Column('has_video', BOOLEAN),
        Column('stripped_thumb', LargeBinary),
        Column('file_reference', LargeBinary),
        schema='public'
    )
    return TgGroup


def get_tg_group(meta):
    TgGroup = Table(
        'tg_groups', meta,
        Column('group_id', BIGINT, primary_key=True, nullable=False),
        Column('join_request', BOOLEAN),
        Column('access_hash', BIGINT),
        Column('participants_count', SMALLINT),
        Column('photo', BIGINT),
        Column('date', TIMESTAMP(timezone=True)),
        Column('verified', BOOLEAN),
        Column('megagroup', BOOLEAN),
        Column('scam', BOOLEAN),
        Column('fake', BOOLEAN),
        Column('gigagroup', BOOLEAN),
        Column('join_to_send', BOOLEAN),
        Column('title', String),
    )
    return TgGroup


def get_tg_msgs(meta):
    TgMsg = Table(
        'tg_msgs', meta,
        Column('msg_id', BIGINT, primary_key=True, nullable=False),
        Column('chat_id', BIGINT),
        Column('date', TIMESTAMP(timezone=True)),
        Column('message', String),
        Column('msg_text', String),
        Column('raw_text', String),
        Column('photo', BIGINT),
        Column('msg_views', BIGINT),
        Column('forwards', BIGINT),
        Column('sender_id', BIGINT),
        Column('sender', String),
        schema='public'
    )
    return TgMsg


async def tg_chat_to_db(t_group, tasks, pg_eng, conn, client):
    #  получим список тем для поиска из файла
    themes = await loop.run_in_executor(None, read_themes_file, 'themes')
    # получим id всех групп которые уже есть в базе
    added_chats = await get_added_chats_id(conn)
    async for theme in AsyncIteratorWrapper(themes):
        # ищем по темам из файла
        answer = await client(functions.contacts.SearchRequest(q=f"Новости {theme}", limit=100))
        async for chat in AsyncIteratorWrapper(answer.chats):
            # ленты которые у нас уже есть нас не интересуют
            if chat.id not in added_chats:
                # чаты пользователей нас не интересуют, только тематические каналы с публикациями
                if not chat.megagroup and not chat.gigagroup:
                    tasks.append(t_group.create_task(pg_eng.add_group_to_db(chat), name="adding chat: " + str(chat.id)))


async def tg_msgs_to_db(t2_group, tasks, pg, conn, client):
    # добавляем сообщения только по тем чатам что есть в базе
    added_chats = await get_added_chats_id(conn)
    async for chat_id in AsyncIteratorWrapper(added_chats):
        chatPeer = PeerChannel(channel_id=chat_id)
        channel_entity = await client.get_entity(chatPeer)
        async for message in client.iter_messages(channel_entity):
            tasks.append(t2_group.create_task(pg.add_msg_to_db(message), name="adding msg: " + str(message.id)))


async def last_tg_msgs_to_db(t2_group, tasks, pg, conn, client):
    # добавляем сообщения только по тем чатам что есть в базе
    added_chats = await get_added_chats_id(conn)
    async for chat_id in AsyncIteratorWrapper(added_chats):
        chatPeer = PeerChannel(channel_id=chat_id)
        channel_entity = await client.get_entity(chatPeer)
        posts = await client(GetHistoryRequest(
            peer=channel_entity,
            limit=100,
            offset_date=datetime.datetime.now(),
            offset_id=0,
            max_id=0,
            min_id=0,
            add_offset=0,
            hash=0))
        async for post in AsyncIteratorWrapper(posts.messages):
            tasks.append(t2_group.create_task(pg.add_msg_to_db(post), name="adding msg: " + str(post.id)))


async def get_dialogs(tg_client, dialogs):
    if tg_client is not None:
        async for dialog in tg_client.iter_dialogs():
            if dialog.name != 'Telegram':
                dialogs.append(dialog)


async def itr_l(dialogs):
    for d in dialogs:
        yield d


async def get_msgs(tg_client, dialogs, msgs):
    async for dialog in itr_l(dialogs):
        msgs = await tg_client.get_messages(dialog, limit=None)


async def first_uploading():
    # инициализируем движок pgsql и параметры подключения
    pg = pg_engine()
    await pg.get_config()
    await pg.create_sqlengine()

    #  инициализируем клиент телеграмма, его параметры, и запустим его
    tgc = tg_client()
    await tgc.get_config()
    tgcs = await tgc.start()

    tasks = []

    if pg.sqlengine is not None:
        async with pg.sqlengine.connect() as conn:
            if tgcs is not None:
                async with tgcs:
                    async with asyncio.TaskGroup() as t1_group:
                        await tg_chat_to_db(t1_group, tasks, pg, conn, tgcs)

                    async with asyncio.TaskGroup() as t2_group:
                        await tg_msgs_to_db(t2_group, tasks, pg, conn, tgcs)

    await asyncio.gather(*tasks, return_exceptions=True)
    await tgcs.disconnect()


async def updating():
    # инициализируем движок pgsql и параметры подключения
    pg = pg_engine()
    await pg.get_config()
    await pg.create_sqlengine()

    #  инициализируем клиент телеграмма, его параметры, и запустим его
    tgc = tg_client()
    await tgc.get_config()
    tgcs = await tgc.start()

    tasks = []

    if pg.sqlengine is not None:
        async with pg.sqlengine.connect() as conn:
            if tgcs is not None:
                async with tgcs:
                    async with asyncio.TaskGroup() as t1_group:
                        await tg_chat_to_db(t1_group, tasks, pg, conn, tgcs)

                    async with asyncio.TaskGroup() as t2_group:
                        await last_tg_msgs_to_db(t2_group, tasks, pg, conn, tgcs)

    await asyncio.gather(*tasks, return_exceptions=True)
    await tgcs.disconnect()


# with closing(asyncio.get_event_loop()) as loop:
#     loop.run_until_complete(first_uploading())
#     print("first upload have ended")

with closing(asyncio.get_event_loop()) as loop:
    asyncio.ensure_future(updating())
    loop.run_forever()