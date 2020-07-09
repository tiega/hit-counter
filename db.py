import sqlite3 as lite
import config
from collections import defaultdict
from datetime import datetime
import re

UNKNOWN_AGENT = config.UNKNOWN_AGENT

# urls table
URL_SCHEMA = """CREATE TABLE IF NOT EXISTS url (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL,
    count INTEGER NOT NULL
);"""

# daily_hits table
HITS_SCHEMA = """CREATE TABLE IF NOT EXISTS daily_hits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    count INTEGER NOT NULL,
    FOREIGN KEY (url_id)
        REFERENCES url (id)
);"""

# user_agents table
UA_SCHEMA = """CREATE TABLE IF NOT EXISTS user_agents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url_id INTEGER NOT NULL,
    user_agent TEXT NOT NULL,
    count INTEGER NOT NULL,
    FOREIGN KEY (url_id)
        REFERENCES url (id)
);"""

class DbAccess:
    """ This provides access to the database to keep track of urls and views """
    def __init__(self, filename):
        """ Setup connection to file and create tables if they don't exist"""
        self.filename = filename
        connection = lite.connect(filename)
        connection.execute('pragma journal_mode=wal')
        connection.execute('PRAGMA foreign_keys=ON')
        cursor = connection.cursor()
        cursor.execute(URL_SCHEMA)
        cursor.execute(HITS_SCHEMA)
        cursor.execute(UA_SCHEMA)
        

    def get_connection(self):
        """ Get the cursor to use in the current thread and remove rows that have expired in views"""
        connection = lite.connect(self.filename)
        return connection

    def getCount(self, connection, url):
        """ Get the count of a particular url """
        cursor = connection.cursor()
        cursor.execute('SELECT count FROM url WHERE url=?', (url,))
        data = cursor.fetchone()
        if data is None:
            return 0
        return data[0]
    
    def getDailyCount(self, connection, url, date=None):
        cursor = connection.cursor()
        if date is None:
            date = datetime.today().strftime("%y-%m-%d")
        elif isinstance(date, datetime):
            date = date.strftime("%y-%m-%d")
        else:
            raise ValueError("date not recognised (must be datetime.datetime format): ", date)

        cursor.execute("""
        SELECT daily_hits.count 
        FROM daily_hits 
        INNER JOIN url
            on url.id = daily_hits.url_id
        WHERE date=date() AND url=?
        """, (url,))
        data = cursor.fetchone()
        if data is None:
            return 0
        return data[0]
    
    def getAgentCount(self, connection, url, agent):
        cursor = connection.cursor()
        if agent is None:
            agent = UNKNOWN_AGENT
        cursor.execute("""
        SELECT user_agent
        FROM user_agents
        INNER JOIN url
            on url.id = user_agents.url_id
        WHERE user_agent=? AND url=?
        """, (agent, url))
        data = cursor.fetchone()
        if data is None:
            return 0
        return data[0]

    def addUrlCount(self, connection, url):
        """ 
        Create url entry if needed
        Increase url count
        """
        cursor = connection.cursor()
        # Make sure the url entry exists
        count = self.getCount(connection, url)
        if count == 0:
            cursor.execute('INSERT INTO url(url, count) VALUES(?, ?)', (url, 0))
        # Add 1 to the url count
        cursor.execute('UPDATE url SET count = count + 1 WHERE url=?', (url, ))

        connection.commit()
    
    def addDailyCount(self, connection, url):
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM url WHERE url=?", (url,))
        url_id = cursor.fetchone()[0]

        # Make sure today's entry exists
        count = self.getDailyCount(connection, url)
        if count == 0:
            cursor.execute("""
            INSERT INTO daily_hits(url_id, date, count) VALUES(?, date(), ?)
            """, (url_id, 0))
        # Add 1 to today
        cursor.execute("""
        UPDATE daily_hits
        SET count = count + 1
        WHERE date=date() AND url_id=?
        """, (url_id,))
    
        connection.commit()
    
    def addAgentCount(self, connection, url, agent):
        if agent is None:
            agent = UNKNOWN_AGENT
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM url WHERE url=?", (url,))
        url_id = cursor.fetchone()[0]

        # Make sure the agent for this url exists
        count = self.getAgentCount(connection, url, agent)
        if count == 0:
            print("Creating for ", url)
            cursor.execute("""
            INSERT INTO user_agents(url_id, user_agent, count)
            VALUES(?, ?, ?)
            """, (url_id, agent, 0))

        # Add 1 to the user agent
        cursor.execute("""
        UPDATE user_agents
        SET count = count + 1
        WHERE url_id=? AND user_agent=?
        """, (url_id, agent))
    
        connection.commit()


    def getTopSites(self, connection, amount=10):
        """ Get the top domains using this tool by hits. Ignore specified domains """
        # Select all urls and counts
        cursor = connection.cursor()
        cursor.execute('select url, count from url')
        urls_and_counts = cursor.fetchall()

        # Get total hits per domain
        site_counts = defaultdict(int)
        for row in urls_and_counts:
            if row[0] == b'':
                continue
            # Get the domain - part before the first '/'
            domain = row[0].split('/')[0]
            # Check if domain is on the ignore list
            on_ignore = False
            for regex in config.TOP_SITES_IGNORE_DOMAIN_RE_MATCH:
                if re.match(regex, domain) is not None:
                    on_ignore = True
                    break
            if on_ignore:
                continue
            # Add hit counts to the domain
            site_counts[domain] += row[1]

        # Sort the domains by hits
        sorted_sites = sorted(site_counts, key=lambda x: site_counts[x], reverse=True)

        # Return sorted domains and their values, this allows for lower Python version support
        return {
            'domains': sorted_sites[:amount],
            'values': {site: site_counts[site] for site in site_counts}
        }
