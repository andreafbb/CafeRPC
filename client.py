import grpc

import cafe_pb2
import cafe_pb2_grpc


def generate_orders():
    customer = "Andrea"
    orders = [
        ("caffe", 2),
        ("cornetto", 1),
        ("succo", 1),   # volutamente non presente nel listino
        ("FINE", 0),
    ]

    for item, qty in orders:
        yield cafe_pb2.OrderRequest(
            customer=customer,
            item=item,
            quantity=qty,
        )


def run():
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = cafe_pb2_grpc.CafeStub(channel)

        for response in stub.OrderStream(generate_orders()):
            print(
                f"[{response.status}] {response.message} | totale parziale: {response.partial_total:.2f} EUR"
            )


if __name__ == "__main__":
    run()