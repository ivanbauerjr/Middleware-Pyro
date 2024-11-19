import Pyro5.api

@Pyro5.api.expose
class Lider:
    def __init__(self):
        self.data = []
        self.subscribers = []

    def get_data(self):
        return self.data

    def publish_message(self, message):
        self.data.append(message)
        print(f"[Líder] Mensagem recebida: {message}")
        self.notify_subscribers()

    #Registra um broker no líder e armazena seu estado (votante ou observador).
    def register_subscriber(self, subscriber_uri, role):
        if subscriber_uri not in self.subscribers:
            self.subscribers[subscriber_uri] = role
            print(f"[Líder] Registrado: {subscriber_uri} como {role}")

    def notify_subscribers(self):
        for subscriber_uri in self.subscribers:
            try:
                subscriber = Pyro5.api.Proxy(subscriber_uri)
                subscriber.update(self.data)
            except Exception as e:
                print(f"[Líder] Falha ao notificar {subscriber_uri}: {e}")

    def send_heartbeat(self):
        for subscriber_uri in self.subscribers:
            try:
                subscriber = Pyro5.api.Proxy(subscriber_uri)
                subscriber.ping()
            except Exception:
                print(f"[Líder] Falha detectada no {subscriber_uri}")
                self.promote_observer()

    def promote_observer(self):
        for subscriber_uri in self.subscribers:
            try:
                subscriber = Pyro5.api.Proxy(subscriber_uri)
                if subscriber.role == "Observador":
                    subscriber.promote_to_voter()
                    return
            except Exception as e:
                print(f"[Líder] Erro ao promover observador: {e}")



def start_leader():
    lider = Lider()
    daemon = Pyro5.api.Daemon()
    uri = daemon.register(lider)
    with Pyro5.api.locate_ns() as ns:
        ns.register("Lider_Epoca1", uri)
    print(f"Líder ativo em {uri}")
    daemon.requestLoop()

if __name__ == "__main__":
    start_leader()
