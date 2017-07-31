import wan_optimizer
import utils
from tcp_packet import Packet
import copy

class WanOptimizer(wan_optimizer.BaseWanOptimizer):
    """ WAN Optimizer that divides data into variable-sized
    blocks based on the contents of the file.

    This WAN optimizer should implement part 2 of project 4.
    """

    # The string of bits to compare the lower order 13 bits of hash to
    GLOBAL_MATCH_BITSTRING = '0111011001010'

    def __init__(self):
        wan_optimizer.BaseWanOptimizer.__init__(self)
        # Add any code that you like here (but do not add any constructor arguments).
        self.send_buffer = {}
        self.hash_to_data = {}
        return

    def send_data_fin(self, data, src, dest):
        while len(data) > utils.MAX_PACKET_SIZE:
            if dest in self.address_to_port:
                self.send(Packet(src, dest, True, False, data[:utils.MAX_PACKET_SIZE]), self.address_to_port[dest])
            else:
                self.send(Packet(src, dest, True, False, data[:utils.MAX_PACKET_SIZE]), self.wan_port)
            data = data[utils.MAX_PACKET_SIZE:]

        if dest in self.address_to_port:
            self.send(Packet(src, dest, True, True, data), self.address_to_port[dest])
        else:
            self.send(Packet(src, dest, True, True, data), self.wan_port)

    # Given a data hash, put it into a packet and send.
    def send_hash(self, data_hash, src, dest, is_fin=False):
        packet = Packet(src, dest, False, is_fin, data_hash)
        if dest in self.address_to_port:
            self.send(packet, self.address_to_port[packet.dest])
        else:
            self.send(packet, self.wan_port)            

    # Add this packet to the buffer.
    def add_to_buffer(self, packet):
        assert (packet.size() <= utils.MAX_PACKET_SIZE), "PACKET TOO BIG"
        if (packet.src, packet.dest) not in self.send_buffer:
            self.send_buffer[(packet.src, packet.dest)] = packet.payload
        else:
            self.send_buffer[(packet.src, packet.dest)] += packet.payload

    def reset_buffer(self, src, dest):
        self.send_buffer[(src, dest)] = ''

    # Pull the first BLOCK_SIZE bytes from this buffer, and leave the rest in
    # returns a list of strings where the size for each is <= the MTU
    def collect_from_buffer(self, src, dest):
        if (src, dest) not in self.send_buffer:
            self.send_buffer[(src, dest)] = ''
        return self.send_buffer[(src, dest)]

    def receive(self, packet):
        """ Handles receiving a packet.

        Right now, this function simply forwards packets to clients (if a packet
        is destined to one of the directly connected clients), or otherwise sends
        packets across the WAN. You should change this function to implement the
        functionality described in part 2.  You are welcome to implement private
        helper fuctions that you call here. You should *not* be calling any functions
        or directly accessing any variables in the other middlebox on the other side of 
        the WAN; this WAN optimizer should operate based only on its own local state
        and packets that have been received.
        """
        #if it's a raw data, always buffer it until it finds delimiter.
        if packet.is_raw_data:
            self.add_to_buffer(packet)
            data = self.collect_from_buffer(packet.src, packet.dest)
            i = 0
            s = 0
            while i + 48 <= len(data):
                lower_13 = utils.get_last_n_bits(utils.get_hash(data[i:i+48]), 13)
                #if it finds delimiter, then send every possible packet that has delimiter
                if lower_13 == self.GLOBAL_MATCH_BITSTRING:
                    self.reset_buffer(packet.src, packet.dest)
                    packet.payload = data[i+48:]
                    self.add_to_buffer(packet)
                    block_string = data[s: i+48]

                    h = utils.get_hash(block_string)
                    if h in self.hash_to_data:
                        self.send_hash(h, packet.src, packet.dest, False)
                    else:
                        self.hash_to_data[h] = block_string
                        while len(block_string) > 0:
                            if packet.dest in self.address_to_port:
                                self.send(Packet(packet.src, packet.dest, True, False, block_string[:utils.MAX_PACKET_SIZE]), self.address_to_port[packet.dest])
                            else:
                                self.send(Packet(packet.src, packet.dest, True, False, block_string[:utils.MAX_PACKET_SIZE]), self.wan_port)
                            block_string = block_string[utils.MAX_PACKET_SIZE:]
                    s = i + 48
                i += 1

            #fin packet, dump it all
            if packet.is_fin:
                data = self.collect_from_buffer(packet.src, packet.dest)
                self.reset_buffer(packet.src, packet.dest)

                h = utils.get_hash(data)
                if h in self.hash_to_data:
                    self.send_hash(h, packet.src, packet.dest, True)
                else:
                    self.hash_to_data[h] = data
                    self.send_data_fin(data, packet.src, packet.dest)
                
        else:
            # case where you receive a hash from the WAN
            # look up the raw data and forward it to the correct address on your LAN
            raw_data = self.hash_to_data[packet.payload]
            while len(raw_data) > utils.MAX_PACKET_SIZE:
                self.send(Packet(packet.src, packet.dest, True, False, raw_data[:utils.MAX_PACKET_SIZE]), self.address_to_port[packet.dest])
                raw_data = raw_data[utils.MAX_PACKET_SIZE:]
            self.send(Packet(packet.src, packet.dest, True, packet.is_fin, raw_data), self.address_to_port[packet.dest])



