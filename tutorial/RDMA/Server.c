// server.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <rdma/rdma_cma.h>

int main() {
    struct rdma_cm_id *listener = NULL, *conn = NULL;
    struct rdma_event_channel *ec = NULL;
    struct rdma_cm_event *event = NULL;
    struct ibv_pd *pd = NULL; // protect domain
    struct ibv_cq *cq = NULL; // completion queue
    struct ibv_qp *qp = NULL; // queue pair
    struct ibv_mr *recv_mr = NULL, *send_mr = NULL; // memory region
    uint64_t recv_buf[2], send_buf[1];
    struct ibv_wc wc; // work completion

    // 1. Setup connection management
    ec = rdma_create_event_channel();
    rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP);
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(7471); // Choose a port
    rdma_bind_addr(listener, (struct sockaddr *)&addr);
    rdma_listen(listener, 10);
    printf("Server listening on port 7471...\n");

    // 2. Wait for connection request
    rdma_get_cm_event(ec, &event);
    if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
        fprintf(stderr, "unexpected event\n");
        return 1;
    }
    conn = event->id;
    rdma_ack_cm_event(event);

    // 3. Create Verbs resources
    pd = ibv_alloc_pd(conn->verbs);
    cq = ibv_create_cq(conn->verbs, 10, NULL, NULL, 0);
    struct ibv_qp_init_attr qp_attr = {
        .send_cq = cq, .recv_cq = cq,
        .cap = { .max_send_wr = 1, .max_recv_wr = 1, .max_send_sge = 1, .max_recv_sge = 1 },
        .qp_type = IBV_QPT_RC
    };
    rdma_create_qp(conn, pd, &qp_attr);
    qp = conn->qp;

    // 4. Register memory
    recv_mr = ibv_reg_mr(pd, recv_buf, sizeof(recv_buf), IBV_ACCESS_LOCAL_WRITE);
    send_mr = ibv_reg_mr(pd, send_buf, sizeof(send_buf), 0);

    // 5. Post a receive buffer to get data
    struct ibv_recv_wr recv_wr = {0}, *bad_recv_wr;
    struct ibv_sge recv_sge = { .addr = (uintptr_t)recv_buf, .length = sizeof(recv_buf), .lkey = recv_mr->lkey };
    recv_wr.wr_id = 0; recv_wr.sg_list = &recv_sge; recv_wr.num_sge = 1;
    ibv_post_recv(qp, &recv_wr, &bad_recv_wr);

    // 6. Accept the connection
    rdma_accept(conn, NULL);
    rdma_get_cm_event(ec, &event); // Wait for ESTABLISHED
    rdma_ack_cm_event(event);
    printf("Connection established.\n");

    // 7. Wait for data
    while (ibv_poll_cq(cq, 1, &wc) == 0);
    printf("Received: %lu + %lu\n", recv_buf[0], recv_buf[1]);

    // 8. Send back the sum
    send_buf[0] = recv_buf[0] + recv_buf[1];
    struct ibv_send_wr send_wr = {0}, *bad_send_wr;
    struct ibv_sge send_sge = { .addr = (uintptr_t)send_buf, .length = sizeof(send_buf), .lkey = send_mr->lkey };
    send_wr.wr_id = 1; send_wr.sg_list = &send_sge; send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_SEND; send_wr.send_flags = IBV_SEND_SIGNALED;
    ibv_post_send(qp, &send_wr, &bad_send_wr);

    // 9. Cleanup
    while (ibv_poll_cq(cq, 1, &wc) == 0);
    printf("Result sent. Exiting.\n");

    ibv_dereg_mr(recv_mr); ibv_dereg_mr(send_mr);
    rdma_destroy_qp(conn); ibv_destroy_cq(cq); ibv_dealloc_pd(pd);
    rdma_destroy_id(listener); rdma_destroy_event_channel(ec);
    return 0;
}
