
class PeerInfo:
    #Class to store all the content of the peers
    def __init__(self, peer_id,host,port,file_exist):
        self.peer_id = int(peer_id)
        self.host = host
        self.port = int(port)
        self.file_exist = bool(int(file_exist))
    def __repr__(self):
        return f"Peer(ID={self.peer_id}, Host={self.host}, Port={self.port}, File={self.file_exist})"

#Read the peerinfo file and store the extracted value in the class
#Each class instance is store in array
def read_peer_info(file_path="PeerInfo.cfg"):
    peers = []
    with open(file_path, "r") as file:
        for line in file:
            parts = line.strip().split()
            if len(parts) == 4:
                peer = PeerInfo(*parts)
                peers.append(peer)
    return peers

#Read the common config file, store the value in a tuple
#It also change the strings to integers
def read_common_config(file_path="Common.cfg"):
    config = {}
    with open(file_path, "r") as file:
        for line in file:
            if not line.strip():
                continue
            key, value = line.strip().split()
            config[key] = value

    numeric_fields = [
         "NumberOfPreferredNeighbors",
        "UnchokingInterval",
        "OptimisticUnchokingInterval",
        "FileSize",
        "PieceSize"
    ]
    for field in numeric_fields:
        if field in config:
            config[field] = int(config[field])

    return config

if __name__ == "__main__":
    # Test parsing both configs
    peers = read_peer_info()
    config = read_common_config()

    #Printing to test the parsing 
    print("\n Common.cfg contents:")
    for k, v in config.items():
        print(f"{k}: {v}")

    print("\n PeerInfo.cfg contents:")
    for p in peers:
        print(p)
