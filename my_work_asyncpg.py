#не критикуем ёпта мне 14-15 лет 
import asyncio
import asyncpg
from datetime import datetime
async def my_work_asyncpg():
    print("добро пожаловать Ерасыл байкеша")
    
    #база данных Neon 
    dsn = "postgresql://neondb_owner:npg_23dWmpUoSVyF@ep-wispy-union-ahj8rdzq-pooler.c-3.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    try: 
    #связь
        pool = await asyncpg.create_pool(dsn=dsn, min_size=1)
        
        #вход ну защита
        async with pool.acquire() as conn:
            
            #работа в базе короче 
            await conn.execute("DROP TABLE IF EXISTS orders;")
            await conn.execute("""
            CREATE TABLE orders (
                user_id INT,
                amount DECIMAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)
            
            #ну вывод на экран скок потратил
            await conn.execute("INSERT INTO orders (user_id, amount) VALUES (1, 17950), (2, 47830), (3, 12000), (4, 3792), (9, 7489);")
            
            #эт анализ и запрос к базе 
            query = "SELECT user_id, SUM(amount) AS total_sum FROM orders WHERE created_at > $1 GROUP BY user_id"
            rows = await conn.fetch(query, datetime(2026, 1, 14))
            
            print("\n РЕЗУЛЬТАТ СКОК ПОТРАТИЛ МОЙ БАЙКЕ МИЛЛИОНЕР ")
            
            
            #основной вывод на экран крч
            for row in rows:
                
                print(f" Ерасыл байке {row['user_id']} потратил в общем за месяц и кайфанул от души : {row['total_sum']} бабосиков")
                print("--------------------------------------------------------------------------|     \n")
                #крч for ради забавы добнул если надо сделаю таких сообщений 100000
            for i in range(0, 1) :
               
                print(" да он гений миллиардер филантроп отсылка на железного чела и Илона маска :], причем мидл+ как я пон\n")
                
            
        await pool.close()
    except Exception as e:
        print(f"Ошибка ИСПРАВЛЯЙ НУРБОЛ: {e}")

if __name__ == "__main__" :
    asyncio.run(my_work_asyncpg())
    #крч ждите около 4-5 сек или хз у мя работает за 5-6 сек
    #Асинхронные эт гроб для мя😀