from __future__ import print_function
from tqdm import tqdm
import pickle
import heapq
import math
import time
import json
import sys


class UserNetwork(object):
    def __init__(self, D=1, T=2, do_flag_purchases=False, debug_mode=False):
        """
        UserNetwork initialization with default parameters
        
        :param D: int, specifies Dth degree connections to be included in user's network
        :param T: int, specifies T recent purchases in network to be considered for anomaly detection
        :param do_flag_purchases: bool, specifies whether to flag anomalous purchase 
        :param debug_mode: bool, specifies whether in debug mode, in debug mode a few more lists will be populated
        """

        # taking the default values for D, T at initialization actual value will be updated by first line in batch_log
        self.D = D
        self.T = T

        # this counter increments as log entry is processed, this is a proxy of unique timestamp
        self.log_entry_counter = 0

        # stores user network, key is user_id, value is the set user's 1st degree connections
        self.network = dict()

        # stores purchase history of individual users, key is user_id, value is a list of 2-item lists in the format
        # [log_entry_counter, purchase amount]
        self.own_purchases = dict()

        self.do_flag_purchases = do_flag_purchases

        # stores the list of flagged purchases, each item is a string in specified format, with mean and sd fields
        self.flagged_purchases = []

        # specifies if in debug mode, only populate the logs below in debug mode
        self.debug_mode = debug_mode

        # some useful debug info for studying complexity
        self.n_friends_log = []  # records # of connected users vs log_entry_counter
        self.n_items_to_merge_log = []  # records total # of items in all relevant purchase history lists
        self.find_friends_time_log = []  # records the time spent on building social circles
        self.merge_time_log = []  # records the time spent on heap merge
        print('UserNetwork debug_mode = {}'.format(self.debug_mode))

        self.flagged_purchase_template = \
            '{{"event_type":"{}", "timestamp":"{}", "id": "{}", "amount": "{}", "mean": "{:.2f}", "sd": "{:.2f}"}}'

    def process_log_entry(self, entry_dict):
        """
        Log entry process function. It processes incoming log entry, one line at a time, as a dictionary entry_dict.
        It first checks log entry validity, if not valid then skip it. 
        For valid entries, it decides whether it is a purchase, or befriend/unfriend and calls appropriate functions 
        
        :param entry_dict: current line of log entry as a dict
        :return: void
        """
        # check validity of log entry
        entry_type = None  # placeholder for determining entry type
        try:
            if set(entry_dict.keys()) == {'D', 'T'}:  # parameter entry to specify D and T
                entry_type = 'params'
            else:
                if set(entry_dict.keys()) == {'event_type', 'timestamp', 'id', 'amount'} \
                        and entry_dict['event_type'] == 'purchase':
                    # purchase entry

                    _ = float(entry_dict['amount'])  # test and see if amount can be converted to float
                    entry_type = 'purchase'
                if set(entry_dict.keys()) == {'event_type', 'timestamp', 'id1', 'id2'}:
                    # befriend/unfriend entry

                    if entry_dict['event_type'] == 'befriend':
                        entry_type = 'befriend'

                    if entry_dict['event_type'] == 'unfriend':
                        entry_type = 'unfriend'
        except:
            pass

        if entry_type is None:  # either no match or raised exception
            print('illegal log entry: {}, skip'.format(entry_dict))
            return
        # end of validity check

        if set(entry_dict.keys()) == {'D', 'T'}:  # set/update D, T parameters
            self.D = int(entry_dict['D'])
            self.T = int(entry_dict['T'])
            print('updated D=={}, T=={}'.format(self.D, self.T))
        elif entry_dict['event_type'] == 'purchase':
            self.add_purchase(entry_dict)  # handles purchase entries

        elif entry_dict['event_type'] == 'befriend':
            self.add_connection(entry_dict)  # handles "befriend" entries

        elif entry_dict['event_type'] == 'unfriend':
            self.remove_connection(entry_dict)  # handles "unfriend" entries

    def add_user_if_new(self, user_id):
        """
        Helper function for checking if user exists, otherwise initialize user in self.network and self.own_purchases
        :param user_id: str, user's id from log entry
        :return: bool, True if user is new, False if user already exists
        """
        if user_id not in self.network:
            self.network[user_id] = []
            self.own_purchases[user_id] = []
            return True  # user is new
        else:
            return False

    def add_connection(self, entry_dict):
        """
        Function to handle befriend activities.
        
        :param entry_dict: current log entry as a dictionary
        :return: void
        """
        p1_id = entry_dict['id1']
        p2_id = entry_dict['id2']
        self.add_user_if_new(p1_id)
        self.add_user_if_new(p2_id)

        # add connection between p1 and p2
        self.network[p1_id].append(p2_id)
        self.network[p2_id].append(p1_id)

    def remove_connection(self, entry_dict):
        """
        Function to handle unfriend activities.
        
        :param entry_dict: current log entry as a dictionary
        :return: void
        """
        p1_id = entry_dict['id1']
        p2_id = entry_dict['id2']
        if p1_id in self.network and p2_id in self.network:
            # remove connection between p1 and p2
            self.network[p1_id].remove(p2_id)
            self.network[p2_id].remove(p1_id)
        else:  # somehow p1 or p2 are not in the network yet we have a unfriend request
            pass  # currently do nothing about this, but we can change this

    def add_purchase(self, entry_dict):
        """
        Function to handle purchase entries
        The key steps are: 
            - add user if new
            - append purchage to user's own purchase history
            - if self.do_flag_purchases == True, meaning we need to flag anomaly purchases:
                - find all the friends in user's D-th network
                - get the purchase histories of each friend (already sorted by log_entry_counter)
                - merge friends' purchase histories while maintaining sorted order, get the latest T entries
                - calculated mean and sd and determine if current purchase is anomalous
                - record flagged purchase in specified format
        :param entry_dict: current log entry as a dictionary
        :return: void
        """
        user_id = entry_dict['id']
        purchase_amount = float(entry_dict['amount'])
        # timestamp = datetime.datetime.strptime(entry_dict['timestamp'], '%Y-%m-%d %H:%M:%S')

        self.add_user_if_new(user_id)  # handle new users

        # now update user's own purchase history, each entry is a 2-item list: [log_entry_counter, purchase amount]
        # the latest entry has the most negative (smallest) log_entry_counter
        self.own_purchases[user_id].append([self.log_entry_counter, purchase_amount])
        self.log_entry_counter -= 1

        if not self.do_flag_purchases:  # stops here if no need to flag purchases
            return

        start_time = time.time()
        # connected users is a set of users in user's social circle up to Dth degree connectivity
        connected_users = self.find_friends(user_id)
        if self.debug_mode:
            self.find_friends_time_log.append(time.time() - start_time)
            self.n_friends_log.append(len(connected_users))
            # record # of connected users in log in debug mode

        start_time = time.time()
        recent_purchases = []
        heap = [[self.own_purchases[x][-1], len(self.own_purchases[x]) - 1, self.own_purchases[x]] for x in
                connected_users if self.own_purchases[x]]
        if self.debug_mode:
            s = 0
            for m in heap:
                s += len(m[2])
            # record total # of items in list of lists
            self.n_items_to_merge_log.append(s)

        heapq.heapify(heap)
        while heap:
            min_pair = heapq.heappop(heap)

            recent_purchases.append(min_pair[0][1])

            if min_pair[1] > 0:
                min_pair[1] -= 1
                min_pair[0] = min_pair[2][min_pair[1]]
                heapq.heappush(heap, min_pair)

            if len(recent_purchases) == self.T:
                break

        if self.debug_mode:
            self.merge_time_log.append(time.time() - start_time)

        # check if purchase should be flagged
        if len(recent_purchases) >= 2:
            self.flag_purchase(recent_purchases, purchase_amount, entry_dict)

    def flag_purchase(self, recent_purchases, purchase_amount, entry_dict):
        """
        Flag anomalous purchase given recent purchase history from network and purchase amount of current purchase
        :param recent_purchases: list of float, recent purchase history from network
        :param purchase_amount: float, current purchase amount
        :param entry_dict: current log entry as a dict
        :return: void
        """
        # print(recent_purchases)
        # current_mean = np.mean(recent_purchases)
        # current_std = np.std(recent_purchases)

        # not using numpy to reduce a dependency
        current_mean = sum(recent_purchases) * 1.0 / len(recent_purchases)
        current_std = math.sqrt(
            sum([(x - current_mean) ** 2 for x in recent_purchases]) * 1.0 / len(recent_purchases))

        if purchase_amount > current_mean + 3 * current_std:
            filled_str = self.flagged_purchase_template.format(entry_dict['event_type'], entry_dict['timestamp'],
                                                               entry_dict['id'], entry_dict['amount'], current_mean,
                                                               current_std)
            self.flagged_purchases.append(filled_str)

    def find_friends(self, user_id):
        """
        Function to find all the friends in user's Dth degree network
        this is a non recursive implementation of breadth first search
        
        :param user_id: str, user's id from log entry
        :return: list, friends in user's Dth degree network as a list
        """
        depth = self.D
        if depth <= 1:
            return

        fronts = [user_id]
        visited_nodes = set()
        while depth > 0:
            new_front = set()
            for f in fronts:
                for friend in self.network[f]:
                    if friend != user_id and friend not in visited_nodes:
                        new_front.add(friend)
            visited_nodes = visited_nodes | new_front
            fronts = new_front
            depth = depth - 1

        return list(visited_nodes)

    def debug_log(self, log_file):
        """
        Output a few logs for debugging purposes
        :param log_file: str, filename of target output log file as a pickle
        :return: void
        """

        log_dict = dict()

        log_dict['N_friends'] = self.n_friends_log
        log_dict['N_items_to_merge'] = self.n_items_to_merge_log
        log_dict['merge_time'] = self.merge_time_log
        log_dict['find_friends_time'] = self.find_friends_time_log

        with open(log_file, 'wb') as f:
            pickle.dump(log_dict, f)


def process_log(filename, network):
    """
    Process either batch_log or stream_log
    
    :param filename: str, file name as a string
    :param network: UserNetwork, an instance of UserNetwork class
    :return: void
    """
    with open(filename, 'r') as f:
        log = f.readlines()
    print('read {}'.format(filename))

    print('processing {}'.format(filename))
    for i in tqdm(range(len(log))):
        try:
            entry_dict = json.loads(log[i])
        except:
            print('failed to parse line {}, skip'.format(i))
            continue
        network.process_log_entry(entry_dict)


def main():
    if len(sys.argv) < 4:
        debug = True
        sample_dir = 'sample_dataset'
        # sample_dir = 'sample2'
        batch_log_file = '../{}/batch_log.json'.format(sample_dir)
        stream_log_file = '../{}/stream_log.json'.format(sample_dir)
        output_file = '../{}/flagged_method.json'.format(sample_dir)
        log_file = '../{}/log.pkl'.format(sample_dir)

    else:
        debug = False
        batch_log_file = sys.argv[1]
        stream_log_file = sys.argv[2]
        output_file = sys.argv[3]

    print('batch_log: {}'.format(batch_log_file))
    print('stream_log: {}'.format(stream_log_file))
    print('output_file: {}'.format(output_file))

    # initialize network
    network = UserNetwork(debug_mode=debug)

    # process batch_log
    network.do_flag_purchases = False
    process_log(batch_log_file, network)

    if network.debug_mode:
        network.debug_log(log_file)

    # process stream log
    network.flagged_purchases = [] # force clean up flagged_purchases list
    network.do_flag_purchases = True
    process_log(stream_log_file, network)

    with open(output_file, 'w') as f:
        f.write('\n'.join(network.flagged_purchases))
    print('output to {}'.format(output_file))


if __name__ == "__main__":
    main()
