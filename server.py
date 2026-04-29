from concurrent import futures
import grpc
import cafe_pb2
import cafe_pb2_grpc

#Prezzario sviluppato con un dizionario

PRICES = {
    "caffe" : 1.20,
    "acqua" : 0.80,
    "cornetto" : 1.50,
    "cappuccino" : 1.50,
    "sigarette" : 5.00,
}

class CafeServicer(cafe_pb2_grpc.CafeServicer):
    def OrderStream(self, request_iterator, context):
        total = 0.0
        total_items = 0
        
        #Iteriamo ora nell'iterator dello stream server
        for request in request_iterator:
            customer = request.customer or "cliente"
            item = request.item.strip().lower() 
            
            """
            Casi particolari gestici con if per lasciare spazio all'aggiornamento normale dell'ordine
            """
            
            #Gestisco il caso in cui il cliente termina l'ordine
            if item == "fine":
                yield cafe_pb2.OrderResponse(
                    status = "SUMMARY",
                    message = f"Ordine terminato, prodotti ordinati: {total_items} , totale parziale: {round(total, 2)}",
                    partial_total = round(total, 2),
                )
                return
            
            qty = int(request.quantity)
            #Gestisco il caso in cui l'inserimento non è corretto
            if item not in PRICES or qty <= 0:
                yield cafe_pb2.OrderResponse(
                    status = "ERROR",
                    message = f"Prodotto non valido oppure quantità errata: {qty}",
                    partial_total = round(total, 2),
                )
                continue
            
            """
            Se invece l'ordine deve proseguire con l'inserimento faccio così
            """
            subtotal = PRICES[item] * qty
            total += subtotal
            total_items += 1
            
            yield cafe_pb2.OrderResponse(
                status = "OK",
                message = f"Prodotto {item} aggiunto all'ordine con quantità {qty}",
                partial_total = round(total, 2),
            )
            
def serve():
    """
    Ora apriamo il server e mettiamolo in collegamento nella porta 50051
    """
    port = "50051"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    cafe_pb2_grpc.add_CafeServicer_to_server(CafeServicer(), server)
    #Bind
    server.add_insecure_port("[::]:" + port)
    #Avvio del server
    server.start() 
    print(f"Server Calcolatrice avviato sulla porta: {port}")
    
    #Attendo terminazione da terminale
    server.wait_for_termination()
    
if __name__ == "__main__":
    serve()
            
                
                
    