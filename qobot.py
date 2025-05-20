import discord
import dotenv
import os
import aiosqlite
import asyncio
import logging
from discord.ext import tasks, commands

from logging.handlers import TimedRotatingFileHandler

log_handler = TimedRotatingFileHandler('qobot.log', encoding="utf8", when="d", interval=7, backupCount=3)
log_handler.setFormatter(logging.Formatter('[%(asctime)s][%(name)s][%(levelname)s] %(message)s'))

logger = logging.getLogger()
logger.addHandler(log_handler)
logger.setLevel(logging.INFO)
logger.info("Application Started")

dotenv.load_dotenv()


async def get_db():
    db = await aiosqlite.connect(os.getenv('QOBOTDB'))
    db.row_factory = aiosqlite.Row
    return db


async def init_db():
    db = await get_db()
    try:
        await db.execute('''CREATE TABLE IF NOT EXISTS users_in_role (user TEXT, date TEXT)''')
        await db.execute('''CREATE UNIQUE INDEX IF NOT EXISTS users_in_role_user_idx ON users_in_role(user)''')
        await db.execute('''CREATE UNIQUE INDEX IF NOT EXISTS users_in_role_date_idx ON users_in_role(date)''')
    finally:
        await db.close()


async def remove_from_db(db, user):
    logger.info(f"Removing user {user} from DB.")
    await db.execute('''DELETE FROM users_in_role WHERE user = ?''', (user,))
    await db.commit()


async def add_user_to_db(db, user):
    logger.info(f"Adding user {user} to DB.")
    await db.execute('''INSERT INTO users_in_role (user, date) VALUES (?, datetime())''', (user,))
    await db.commit()


async def update_probation_users(users_in_probation):
    users_in_db = set()
    db = await get_db()
    try:
        async with db.execute('SELECT user FROM users_in_role') as cursor:
            async for row in cursor:
                users_in_db.add(int(row['user']))
        
        missing_users = users_in_probation - users_in_db
        stale_users = users_in_db - users_in_probation

        for user in stale_users:
            await remove_from_db(db, user)
        
        for user in missing_users:
            await add_user_to_db(db, user)
    finally:
        await db.close()


async def get_ready_users():
    ready_users = set()

    db = await get_db()
    try:
        async with db.execute('''SELECT user FROM users_in_role WHERE date < datetime('now', '-7 day')''') as cursor:
            async for row in cursor:
                ready_users.add(int(row['user']))
    finally:
        await db.close()

    return ready_users


role_id_monitored = int(os.getenv("QOBOTROLEID"))
role_id_validated = int(os.getenv("QOBOTVALIDATEDROLEID"))


class ProbationCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.scan_users.start()
        self.promote_users.start()

    def cog_unload(self):
        self.scan_users.cancel()
        self.promote_users.cancel()

    @tasks.loop(minutes=15.0)
    async def scan_users(self):
        logger.info("Scanning for missing role changes.")
        for guild in self.bot.guilds:
            try:
                role = await guild.fetch_role(role_id_monitored)
                users_in_probation = set([member.id for member in role.members])
                await update_probation_users(users_in_probation)
            except (discord.NotFound, discord.HTTPException):
                pass

    @scan_users.before_loop
    async def before_scan_users(self):
        await self.bot.wait_until_ready()
    
    @tasks.loop(minutes=60.0)
    async def promote_users(self):
        logger.info("Entering promotion loop.")
        ready_users = await get_ready_users()
        logger.info(f"Got {len(ready_users)} users: {ready_users}")
        if len(ready_users) < 1:
            return
        for guild in self.bot.guilds:
            try:
                role = await guild.fetch_role(role_id_monitored)
                target_role = await guild.fetch_role(role_id_validated)
                logger.debug(f"Found roles {role} and {target_role}")
                logger.debug(f"Role members: {role.members}")
                for member in role.members:
                    for user_id in ready_users:
                        logger.debug(f"User id: {user_id} and member_id: {member.id}")
                        if member.id == user_id:
                            logger.info(f"Updating user {user_id} with roles.")
                            # XXX Dry run
                            #await member.add_roles(target_role)
                            #await member.remove_roles(role)
                            db = await get_db()
                            try: 
                                await remove_from_db(db, user_id)
                            finally:
                                await db.close()

            except (discord.NotFound, discord.HTTPException):
                pass


    @promote_users.before_loop
    async def before_promote_users(self):
        await self.bot.wait_until_ready()


intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot('!!', intents=intents)


@bot.event
async def on_ready():
    logger.info(f'We have logged in as {bot.user}')

@bot.event
async def on_member_update(before, after):
    if before.roles == after.roles:
        return
    
    before_roles = set(before.roles)
    after_roles = set(after.roles)

    added_roles = after_roles - before_roles
    removed_roles = before_roles - after_roles

    for role in added_roles:
        if role.id == role_id_monitored:
            try:
                db = await get_db()
                await add_user_to_db(db, after.id)
            finally:
                await db.close()

    for role in removed_roles:
        if role.id == role_id_monitored:
            try:
                db = await get_db()
                await remove_from_db(db, after.id)
            finally:
                await db.close()


def main():
    async def runner():
        async with bot:
            await init_db()
            await bot.add_cog(ProbationCog(bot))
            await bot.start(os.getenv("TOKEN"), reconnect=True)

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        # nothing to do here
        # `asyncio.run` handles the loop cleanup
        # and `self.start` closes all sockets and the HTTPClient instance.
        return


if __name__ == '__main__':
    main()
