import wan_optimizer
import utils
from tcp_packet import Packet

class WanOptimizer(wan_optimizer.BaseWanOptimizer):
    """ WAN Optimizer that divides data into fixed-size blocks.

    This WAN optimizer should implement part 1 of project 4.
    """

    # Size of blocks to store, and send only the hash when the block has been
    # sent previously
    BLOCK_SIZE = 8000

    def __init__(self):
        wan_optimizer.BaseWanOptimizer.__init__(self)
        # Add any code that you like here (but do not add any constructor arguments).

        #a (source, dest addrs) => send buffer
        #let a buffer be an ordered list of payloads [a,b,c] a+b+c
        self.send_buffer = {}
        #hash to the data block
        self.hash_to_data = {}
        return

    # Return the size in bytes of a send buffer.
    def buffer_size(self, buf):
        s = 0
        if buf:
            for payload in buf:
                s += len(payload)
        return s

    def get_buffer(self, src, dest):
        if (src, dest) not in self.send_buffer:
            self.send_buffer[(src, dest)] = []
        return self.send_buffer[(src, dest)]

    def reset_buffer(self, src, dest):
        self.send_buffer[(src, dest)] = []

    # Given a list of payloads forming a block, turn each of them
    # into a packet and send. Also want to save the hash.
    def send_block(self, block, src, dest, is_fin=False):
        #DONT FORGET TO COMPUTE THE HASH AND STORE IT
        full_data_string = "".join(block)
        self.hash_to_data[utils.get_hash(full_data_string)] = block
        for i, payload in enumerate(block):
            packet = Packet(src, dest, True, is_fin and i == len(block)-1, payload)
            if dest in self.address_to_port:
                self.send(packet, self.address_to_port[packet.dest])
            else:
                self.send(packet, self.wan_port)

    # Given a data hash, put it into a packet and send.
    def send_hash(self, data_hash, src, dest, is_fin=False):
        packet = Packet(src, dest, False, is_fin, data_hash)
        if dest in self.address_to_port:
            self.send(packet, self.address_to_port[packet.dest])
        else:
            self.send(packet, self.wan_port)

    # Add this packet to the buffer.
    # NEED SOMETHING FOR FIN PACKETS?
    def add_to_buffer(self, packet):
        assert (packet.size() <= utils.MAX_PACKET_SIZE), "PACKET TOO BIG"
        self.send_buffer[(packet.src, packet.dest)].append(packet.payload)

    # Pull the first BLOCK_SIZE bytes from this buffer, and leave the rest in
    # returns a list of strings where the size for each is <= the MTU
    def collect_from_buffer(self, src, dest):
        return self.send_buffer[(src, dest)]


    def receive(self, packet):
        """ Handles receiving a packet.

        Right now, this function simply forwards packets to clients (if a packet
        is destined to one of the directly connected clients), or otherwise sends
        packets across the WAN. You should change this function to implement the
        functionality described in part 1.  You are welcome to implement private
        helper fuctions that you call here. You should *not* be calling any functions
        or directly accessing any variables in the other middlebox on the other side of 
        the WAN; this WAN optimizer should operate based only on its own local state
        and packets that have been received.
        """
        #always buffer raw data until ready to send (full buffer or FIN packet)
        #this raw data could be from someone on your LAN or from the WAN, either way
        #you buffer and forward to the right address when the buffer is filled
        #or the packet is a FIN packet
        if packet.is_raw_data:
            current_buffer_size = self.buffer_size(self.get_buffer(packet.src, packet.dest))
            if current_buffer_size + packet.size() < self.BLOCK_SIZE:
                self.add_to_buffer(packet)
                #fin packet, dump it all
                if packet.is_fin:
                    to_send = self.collect_from_buffer(packet.src, packet.dest)
                    self.reset_buffer(packet.src, packet.dest)
                    full_data_string = "".join(to_send)
                    h = utils.get_hash(full_data_string)
                    if h in self.hash_to_data:
                        self.send_hash(h, packet.src, packet.dest, True)
                    else:
                        self.hash_to_data[h] = to_send
                        self.send_block(to_send, packet.src, packet.dest, True)
            else:
                # buffer full, time to send
                to_send = self.collect_from_buffer(packet.src, packet.dest)
                #split up this packet's payload: the first few bytes become
                #added to this block, the rest get stuffed in later
                boundary = self.BLOCK_SIZE - current_buffer_size
                high_half = packet.payload[0:boundary]
                low_half = packet.payload[boundary:]
                to_send.append(high_half)
                self.reset_buffer(packet.src, packet.dest)

                #Send the first block
                full_data_string = "".join(to_send)
                h = utils.get_hash(full_data_string)
                if h in self.hash_to_data:
                    self.send_hash(h, packet.src, packet.dest)
                else:
                    self.hash_to_data[h] = to_send
                    self.send_block(to_send, packet.src, packet.dest)

                #buffer the rest of the packet, else hash/send it as a second block if FIN
                if not packet.is_fin:
                    self.add_to_buffer(Packet(packet.src, packet.dest, True, False, low_half))
                else:
                    h2 = utils.get_hash(low_half)
                    if h2 in self.hash_to_data:
                        self.send_hash(h2, packet.src, packet.dest, True)
                    else:
                        self.send_block([low_half], packet.src, packet.dest, True)

        else:
            # case where you receive a hash from the WAN
            # look up the raw data and forward it to the correct address on your LAN
            if packet.payload not in self.hash_to_data:
                print("REEEEE THIS SHOULDN'T HAPPEN!" + str(packet))
            raw_data = self.hash_to_data[packet.payload]
            self.send_block(raw_data, packet.src, packet.dest, packet.is_fin)
