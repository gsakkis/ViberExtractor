# ViberExtractor
A python 3 script for extracting messages from Viber Desktop's SQLite message database.

If you use Viber Desktop, all your messages are stored in a SQLite database. On windows, this is
usually located at `C:\Users\*USERNAME*\AppData\Roaming\ViberPC\*YOURPHONE#*\viber.db`

## Usage

    usage: viber.py [-h] [-c CHAT] [-f FROM] [-t TO] [-z TIMEZONE] [-s M] db

    Extract messages from a given SQLite database of Viber message logs.

    positional arguments:
      db                    path to the Viber database file

    optional arguments:
      -h, --help            show this help message and exit
      -c CHAT, --chat CHAT  chatID of the chat to extract messages from
      -f FROM, --from FROM  start date(-time) to filter from
      -t TO, --to TO        end date(-time) to filter to
      -z TIMEZONE, --timezone TIMEZONE
                            convert timestamps to the given timezone; defaults to
                            local timezone
      -s M, --session M     split the chat log into sessions separated by at least
                            M minutes of inactivity
