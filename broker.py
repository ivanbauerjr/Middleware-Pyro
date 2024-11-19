import Pyro5.api
from threading import Thread
import time

@Pyro5.api.expose
class Broker:
    def __init__(self, name, role):
        self.name = name
        self.role = role
        self.data = []

    def update_log(self, leader_log):
        self.data = leader_log
        print(f"[{self.role} {self.name}] Log atualizado: {self.data}")

    def ping(self):
        print(f"[{self.role} {self.name}] Heartbeat recebido do líder.")

    def send_heartbeat(self, leader_uri):
        leader = Pyro5.api.Proxy(leader_uri)
        while True:
            try:
                time.sleep(3)
                leader.register_heartbeat(Pyro5.api.current_context.client)
                print(f"[{self.role} {self.name}] Heartbeat enviado.")
            except Exception as e:
                print(f"[{self.role} {self.name}] Erro ao enviar heartbeat: {e}")
                break

    def promote_to_voter(self):
        self.role = "Votante"
        print(f"[{self.role} {self.name}] Promovido a votante.")

def start_broker(name, role):
    broker = Broker(name, role)
    daemon = Pyro5.api.Daemon()
    uri = daemon.register(broker)

    with Pyro5.api.locate_ns() as ns:
        leader_uri = ns.lookup("Lider_Epoca1")
        leader = Pyro5.api.Proxy(leader_uri)

        # Passa a URI e o papel do broker (votante ou observador)
        leader.register_subscriber(str(uri), role)
        print(f"[{role} {name}] Registrado no líder em {uri}")

        # Inicia heartbeat para brokers votantes
        if role != "Observador":
            Thread(target=broker.send_heartbeat, args=(leader_uri,)).start()

    daemon.requestLoop()



if __name__ == "__main__":
    Thread(target=start_broker, args=("Votante1", "Votante")).start()
    Thread(target=start_broker, args=("Votante2", "Votante")).start()
    Thread(target=start_broker, args=("Observador1", "Observador")).start()