# Python standard
from datetime import datetime

# Third party
from pytz import timezone

TIMEZONE_LONDON: timezone = timezone("Europe/London")

# TD message types

C_BERTH_STEP = "CA"       # Berth step      - description moves from "from" berth into "to", "from" berth is erased
C_BERTH_CANCEL = "CB"     # Berth cancel    - description is erased from "from" berth
C_BERTH_INTERPOSE = "CC"  # Berth interpose - description is inserted into the "to" berth, previous contents erased
C_HEARTBEAT = "CT"        # Heartbeat       - sent periodically by a train describer

S_SIGNALLING_UDPATE = "SF"            # Signalling update
S_SIGNALLING_REFRESH = "SG"           # Signalling refresh
S_SIGNALLING_REFRESH_FINISHED = "SH"  # Signalling refresh finished

def print_td_frame(parsed_body):
    # Each message in the queue is a JSON array
    for outer_message in parsed_body:
        # Each list element consists of a dict with a single entry - our real target - e.g. {"CA_MSG": {...}}
        message = list(outer_message.values())[0]

        message_type = message["msg_type"]

        # For the sake of demonstration, we're only displaying C-class messages
        if message_type in [C_BERTH_STEP, C_BERTH_CANCEL, C_BERTH_INTERPOSE]:
            # The feed time is in milliseconds, but python takes timestamps in seconds
            timestamp = int(message["time"]) / 1000

            area_id = message["area_id"]
            description = message.get("descr", "")
            from_berth = message.get("from", "")
            to_berth = message.get("to", "")

            utc_datetime = datetime.utcfromtimestamp(timestamp)
            uk_datetime = TIMEZONE_LONDON.fromutc(utc_datetime)

            print("{} [{:2}] {:2} {:4} {:>5}->{:5}".format(
                uk_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                message_type, area_id, description, from_berth, to_berth,
            ))

# New code to get train data for the berths we are interested in

def getReqdTrainData(parsed_body, areas, berths, trainids):

    CMESSAGETYPES = [C_BERTH_STEP, C_BERTH_CANCEL, C_BERTH_INTERPOSE, C_HEARTBEAT]

    # Each message in the queue is a JSON array
    for outer_message in parsed_body:
        # Each list element consists of a dict with a single entry - our real target - e.g. {"CA_MSG": {...}}
        message = list(outer_message.values())[0]

        message_type = message["msg_type"]

        # The feed time is in milliseconds, but python takes timestamps in seconds
        timestamp = int(message["time"]) / 1000
        area_id = message["area_id"]

        if area_id in areas:

            if message_type in CMESSAGETYPES:

                found = False
                description = message.get("descr", "")
                from_berth = message.get("from", "")
                to_berth = message.get("to", "")

                if from_berth in berths:
                    found = True

                if to_berth in berths:
                    found = True

            else:

                found = False

                try:
                    description = message['data']
                except:
                    description = ''

                from_berth = message.get("address", "")
                to_berth = ''

            if found:

                utc_datetime = datetime.utcfromtimestamp(timestamp)
                uk_datetime = TIMEZONE_LONDON.fromutc(utc_datetime)
                strdatetime = uk_datetime.strftime("%Y-%m-%d %H:%M:%S")

                print(f"{strdatetime} # {message} # [{message_type}] Area={area_id} Desc={description} Berths {from_berth}->{to_berth}")

