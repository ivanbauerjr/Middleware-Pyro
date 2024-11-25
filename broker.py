import Pyro5.api
from threading import Thread
import time

@Pyro5.api.expose
class Broker:
    def __init__(self, name, role):
        self.name = name
        self.role = role
        self.uri = None
        self.data = []
        self.epoch = 1  # Época inicial
        self.offset = 0  # Offset inicial
        self.voters = []
        
    def get_role(self):
        return self.role  # Método para retornar o role do broker

    def update_log(self, leader_log, leader_epoch):
        if leader_epoch >= self.epoch:  # Atualiza apenas se a época do líder for igual ou mais recente
            self.data = leader_log
            self.epoch = leader_epoch
            self.offset = len(leader_log)
            print(f"[{self.role} {self.name}] Log atualizado: {self.data}, Época: {self.epoch}, Offset: {self.offset}")
        else:
            print(f"[{self.role} {self.name}] Log não atualizado. Época local: {self.epoch}, Época do líder: {leader_epoch}")

    def replicate_log(self, message, leader_epoch):
        if leader_epoch == self.epoch:  # Verifica se a época é consistente
            self.data.append(message)  # Adiciona ao log local
            self.offset += 1
            print(f"[Votante {self.name}] Mensagem replicada: {message}, Offset: {self.offset}")
            # Envia confirmação ao líder com o URI do votante
            try:
                with Pyro5.api.locate_ns() as ns:
                    leader_uri = ns.lookup(f"Lider_Epoca{self.epoch}")
                    leader = Pyro5.api.Proxy(leader_uri)
                    leader.confirm_commit(self.offset, str(self.uri))  # Passa o URI
            except Exception as e:
                print(f"[Votante {self.name}] Falha ao confirmar mensagem: {e}")
        else:
            print(f"[Votante {self.name}] Época inconsistente. Local: {self.epoch}, Líder: {leader_epoch}")



    # Envia heartbeats periódicos ao líder.
    def send_heartbeat(self, leader_uri):
        leader = Pyro5.api.Proxy(leader_uri)
        while True:
            try:
                leader.register_heartbeat(str(self.uri))  # Envia a URI como string
                print(f"[{self.role} {self.name}] Heartbeat enviado para o líder.")
            except Exception as e:
                print(f"[{self.role} {self.name}] Erro ao enviar heartbeat: {e}")
            time.sleep(9)  # Ajuste o intervalo de envio do heartbeat
            # time.sleep(22)  # Teste para fazer o temporizador falhar


    def promote_to_voter(self):
        self.role = "Votante"
        print(f"[{self.role} {self.name}] Promovido a votante.")
        # O novo votante deverá solicitar dados ao líder para garantir que possui a versão mais recente dos dados.
        with Pyro5.api.locate_ns() as ns:
            leader_uri = ns.lookup("Lider_Epoca1")
            leader = Pyro5.api.Proxy(leader_uri)
            leader.register_subscriber(str(self.uri))
            self.update_log(leader.get_confirmed_messages())

    def update_voter_list(self, voter_list):
        self.voters = voter_list
        print(f"[{self.role} {self.name}] Lista de participantes atualizada: {self.voters}")

def start_broker(name, role):
    print(f"Iniciando broker {role} {name}...")  # Print indicando que a thread foi iniciada
    broker = Broker(name, role)
    daemon = Pyro5.api.Daemon()
    uri = daemon.register(broker)
    broker.uri = uri  # Atribui a URI ao broker

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
    print(f"Broker {role} {name} terminado.")  # Print indicando que a execução do broker terminou


if __name__ == "__main__":
    print("Iniciando threads para os brokers...")  # Indicação de que as threads estão sendo iniciadas
    Thread(target=start_broker, args=("Votante1", "Votante")).start()
    Thread(target=start_broker, args=("Votante2", "Votante")).start()
    Thread(target=start_broker, args=("Observador1", "Observador")).start()
    print("Threads de brokers iniciadas.")  # Indicação de que todas as threads foram lançadas
