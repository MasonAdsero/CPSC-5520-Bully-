
'''
Mason Adsero
lab2.py
10/11/2022
'''

import socket
import selectors
import datetime as dt
import pickle
from enum import Enum
import sys

BUF_SZ = 1024
ASSUME_FAILURE_TIMEOUT = 2
CHECK_INTERVAL = 1
PEER_DIGITS = 100

class State(Enum):
    """
    Enumeration of states a peer can be in for the Lab2 class.
    """
    QUIESCENT = 'QUIESCENT'  # Erase any memory of this peer

    # Outgoing message is pending
    SEND_ELECTION = 'ELECTION'
    SEND_VICTORY = 'COORDINATOR'
    SEND_OK = 'OK'

    # Incoming message is pending
    WAITING_FOR_OK = 'WAIT_OK'  # When I've sent them an ELECTION message
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER?'  # This one only applies to myself
    WAITING_FOR_ANY_MESSAGE = 'WAITING'  # When I've done an accept on their connect to my server

    def is_incoming(self):
        """Categorization helper."""
        return self not in (State.SEND_ELECTION, State.SEND_VICTORY, State.SEND_OK)

class bully(object):

    def __init__(self, gcd_address, next_birthday, su_id):
        self.gcd_address = (gcd_address[0], int(gcd_address[1]))
        days_to_birthday = (next_birthday - dt.datetime.now()).days
        self.pid = (days_to_birthday, int(su_id))
        self.members = {}
        self.states = {}
        self.bully = None
        self.selector = selectors.DefaultSelector()
        self.listener, self.listener_address = self.start_a_server()

    
    def run(self):
        '''
        Runs the bully loop

        '''
        self.join_group()
        self.selector.register(self.listener, selectors.EVENT_READ | selectors.EVENT_WRITE)
        self.start_election("I Just joined the group")
        while True:
            events = self.selector.select(CHECK_INTERVAL)
            for key, mask in events:
                if key.fileobj == self.listener:
                    self.accept_peer()
                elif mask & selectors.EVENT_READ:
                    self.receive_message(key.fileobj)
                else:
                    self.send_message(key.fileobj)
            self.check_timeouts()
        return 0

    #Given
    def accept_peer(self):
        '''
        Accepts a connection to our listening socket or self.listener
        '''
        try:
            peer, addr = self.listener.accept()
            peer.setblocking(False)
            print(self.pr_sock(peer) + " accepted " + self.pr_now())
            self.selector.register(peer, selectors.EVENT_READ)
            self.set_state(State.WAITING_FOR_ANY_MESSAGE, peer)
        except Exception as err:
            print("Accept Failed: " + str(err))


    #Given
    def send_message(self, peer):
        '''
        Sends a message to passed in peer
        param peer: socket data is sent to
        '''
        state = self.get_state(peer)
        print('{}: sending {} [{}]'.format(self.pr_sock(peer), state.value, self.pr_now()))
        try:
            self.send(peer, state.value, self.members)
        except ConnectionError as err:
            print(err)
        except Exception as err:
            print(err)
        if state == State.SEND_ELECTION:
            self.set_state(state.WAITING_FOR_OK, peer, switch_mode=True)
        else:
            self.set_quiescent(peer)

    #consulted Justin Thoreson for setting the correct states 
    def receive_message(self, peer):
        '''
        receives a message and handles expected behaivor from the received message
        param peer: socket to receive from
        '''
        try:
            data = self.receive(peer)
        except ConnectionError as err:
            print(err)
        except Exception as err:
            print(err)
        else:
            message, members = data
            print('{}: received {} [{}]'.format(self.pr_sock(peer), message, self.pr_now()))
            if message == "ELECTION":
                self.update_members(members)
                self.send(peer, State.SEND_OK.value)
                print(f"{self.pr_sock(peer)} {self.get_state(peer).value}")
                self.set_state(State.SEND_OK, peer, True)
                if not self.is_election_in_progress():
                    self.set_leader(None)
                    self.start_election("Got a Vote card from a lower-pid peer")
            elif message == "OK":
                self.set_state(State.WAITING_FOR_VICTOR, self.listener)
                print(f"self: {self.get_state(self.listener).value}")
                self.set_quiescent(peer)
            elif message == "COORDINATOR":
                self.update_members(members)
                self.set_state(State.WAITING_FOR_ANY_MESSAGE)
                print(f"self: {self.get_state(self.listener).value}")
                self.set_quiescent(peer)
                self.set_leader(max(members.keys()))
                print("New Leader is: " + str(self.bully))
        
        
    #Consulted Justin Thoreson on how he used is_expired
    def check_timeouts(self):
        '''
        Checks whether messages we expect are coming or not and handles the failure for
        the selector loop
        '''
        for peer in self.states:
            if peer != self.listener and self.is_expired(peer):
                self.set_quiescent(peer)
        state, time = self.get_state(self.listener, True)
        if self.is_expired(time, self.listener):
            if state == State.WAITING_FOR_OK:
                self.declare_victory("No ok response received")
            elif state == State.WAITING_FOR_VICTOR:
                self.start_election(" no COORDINATE response never received")

    def get_connection(self, member):
        '''
        Gets a connection to passed in member else prints failure
        param member: socket hostname and port we want to connect too
        '''
        host, port = member
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((host, port))
            sock.setblocking(False)
            self.selector.register(sock, selectors.EVENT_READ)
            return sock
        except Exception as err:
            print("Failed to connect: " + str(err))
        

    def is_election_in_progress(self):
        '''
        Returns whether an election is in progress or not
        '''
        state = self.states[self.listener][0]
        return state in [State.SEND_ELECTION, State.SEND_OK, State.WAITING_FOR_OK, State.WAITING_FOR_VICTOR]

    def is_expired(self, time, peer=None, threshold=ASSUME_FAILURE_TIMEOUT):
        '''
        Checks whether a peers timestamp for a state crosses the failure threshold
        param time: timestamp of state
        param peer: socket we are getting a timestamp from
        param threshold: the amount of time before a socket is considered expired
        '''
        if peer is None:
            return False
        return abs(dt.datetime.timestamp(dt.datetime.now()) - dt.datetime.timestamp(time)) >= threshold

    def set_leader(self, new_leader):
        '''
        Sets a new bully
        param new_leader: the new bully's pid
        '''
        self.bully = new_leader

    def get_state(self, peer=None, detail=False):
        '''
        Returns current state of a peer
        param peer: socket whose state we want
        param detail: determines if time stamp is returned with state
        '''
        if peer is None:
            peer = self.listener
        status = self.states[peer] if peer in self.states else (State.QUIESCENT, None)
        return status if detail else status[0]

    def set_state(self, state, peer=None, switch_mode=False):
        '''
        Sets the state of a socket in the states dictionary
        param state: state to update the dictionary with
        param peer: socket whose state is being updated
        param switch_mode: determines if a state change needs to result in selector reregistration
        '''
        if peer is None:
            peer = self.listener
        if switch_mode:
            if state.is_incoming():
                self.selector.modify(peer, selectors.EVENT_READ)
            else:
                self.selector.modify(peer, selectors.EVENT_WRITE)
        self.states[peer] = (state, dt.datetime.now())

        

    def set_quiescent(self, peer=None):
        '''
        Removes peer from state and unregisters it
        param peer: socket being removed
        '''
        if not peer or peer is self.listener:
            return
        if peer:
            print(f"{self.pr_sock(peer)}: QUIESCENT")
            self.selector.unregister(peer)
            del self.states[peer]
            peer.close()
        #Unregister socket, out of state, close socket, keep around

    def start_election(self, reason):
        '''
        Starts an election and sends election to greater pids
        param reason: logging variable
        '''
        self.set_state(State.SEND_ELECTION, self.listener)
        print("Starting an election because " + reason)
        #How to check if I am the greatest key
        for key in self.members:
            if key[0] > self.pid[0]:
                peer = self.get_connection(self.members[key])
                self.set_state(State.SEND_ELECTION, peer, True)
                print(f"{self.pr_sock(peer)} {self.get_state(peer).value}")
            elif key[1] > self.pid[1] and key[0] == self.pid[0]:
                peer = self.get_connection(self.members[key])
                self.set_state(State.SEND_ELECTION, peer, True)
                print(f"{self.pr_sock(peer)} {self.get_state(peer).value}")
        if self.pid == max(self.members.keys()):
            self.declare_victory("I have the highest pid")
            return
        self.set_state( State.WAITING_FOR_OK, switch_mode=True)
        print(f"self: {self.get_state(self.listener).value}" )
        

    #Consulted Justin Thoreson on how to set listner state
    def declare_victory(self, reason):
        '''
        Sends coordinate message
        param reason: variable for logging purposes
        '''
        self.set_state(State.QUIESCENT, switch_mode=True)
        for key in self.members:
            if key < self.pid:
                peer = self.get_connection(self.members[key])
                if peer:
                    self.set_state(State.SEND_VICTORY, peer, True)
        print("Victory (" + str(self.pid) + ") " + reason)
        self.set_leader(self.pid)

    def update_members(self, their_idea_of_membership):
        '''
        Updates membership with missing members from another nodes member list
        param their_idea_of_membership: peers member list
        '''
        for member in their_idea_of_membership:
            self.members[member] = their_idea_of_membership[member]

    @classmethod
    def send(cls, peer, message_name, message_data=None, wait_for_reply=False, bufferSize=BUF_SZ):
        '''
        Sends a message to a given peer
        param peer: socket data is being sent too
        param message_name: Name of message such as OK
        param message_data: data sent with message usually members
        param wait_for_reply: Boolean which determines if we wait for a reply
        param bufferSize: dictates data buffer size
        '''
        peer.sendall(pickle.dumps((message_name, message_data)))
        if wait_for_reply:
            return cls.receive(peer, bufferSize)

    
    #Done
    @staticmethod
    def receive(peer, bufferSize=BUF_SZ):
        '''
        Recieves message data
        param peer: socket data will be received from
        param bufferSize: Size of data buffer
        '''
        return pickle.loads(peer.recv(bufferSize))

    @staticmethod
    def start_a_server():
        '''
        Starts a listening socket to recieve messages from other peers
        '''
        addr = ('localhost', 0)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        sock.bind(addr)
        sock.listen(6)
        return (sock, sock.getsockname())

    def join_group(self):
        '''
        Joins the GCD and recieves members
        '''
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as gcd:
            print("Join: " + str(self.gcd_address))
            gcd.connect(self.gcd_address)
            self.members = self.send(gcd, "JOIN", (self.pid, self.listener_address), True)

    @staticmethod
    def pr_now():
        return dt.datetime.now().strftime('%H:%M:%S.%f')

    def pr_sock(self, sock):
        if sock is None or sock == self or sock == self.listener:
            return 'self'
        return self.cpr_sock(sock)

    @staticmethod
    def cpr_sock(sock):
        l_port = sock.getsockname()[1] % PEER_DIGITS
        try:
            r_port = sock.getpeername()[1] % PEER_DIGITS
        except OSError:
            r_port = '???'
        return '{}->{} ({})'.format(l_port, r_port, id(sock))

    def pr_leader(self):
        return 'unknown' if self.bully is None else ('self' if self.bully == self.pid else self.bully)

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Usage: Python Lab2.py exepected 4 arguments: Hostname, port, birthday: yyyy-mm-dd, suid")
    print(sys.argv)
    birthDay = dt.datetime.strptime(sys.argv[3], '%Y-%m-%d')
    peer = bully((sys.argv[1], sys.argv[2]), birthDay, sys.argv[4])
    peer.run() 