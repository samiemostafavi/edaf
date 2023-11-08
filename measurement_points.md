# UE Uplink

`pdcp_data_req` function in `openair2/LAYER2/nr_pdcp/nr_pdcp_oai_api.c:1402` 

`pdcp_data_req_drb` function in `openair2/LAYER2/nr_pdcp/nr_pdcp_oai_api.c:1329` 

`nr_pdcp_entity_recv_sdu` function in `openair2/LAYER2/nr_pdcp/nr_pdcp_entity.c:179`

`deliver_pdu_drb` function in `openair2/LAYER2/nr_pdcp/nr_pdcp_oai_api.c:664`

`enqueue_rlc_data_req` function in `openair2/LAYER2/nr_pdcp/nr_pdcp_oai_api.c:152`

`rlc_data_req_thread` thread in `openair2/LAYER2/nr_pdcp/nr_pdcp_oai_api.c:106` is a thread that in a while loop, it calls RLC apis `rlc_data_req` in file `openair2/LAYER2/nr_rlc/nr_rlc_oai_api.c:349`

In there, it calls `rb->recv_sdu` where `rb` is an `nr_rlc_entity_t`. In file `nr_rlc_entity.c:98` this is set to `nr_rlc_entity_am_recv_sdu` which is defined in file `nr_rlc_entity_am.c:1676`.
This function creats a new RLC SDU and appends it to the RLC queue (tx_list)

On the other end, nrUE scheduler on MAC layer works based on grants and etc to send ulsch packets.

In particular, `nr_ue_get_sdu` function in `openair2/LAYER2/NR_MAC_UE/nr_ue_scheduler.c:3143` does that.
In this function, a `tx_req` is created and filled with the PDUs (lines 1234-1240). One of them is the PDU from UE on ULSCH (line 1224). Then, it is wrapped by `scheduled_response` and this important line is run: `mac->if_module->scheduled_response(&scheduled_response)`.
Here the interface module is an interface to the physical layer. This function `scheduled_response` which is called, is implemented by `nr_ue_scheduled_response` in file `openair1/SCHED_NR_UE/fapi_nr_ue_l1.c:321`.

Function `nr_ue_get_sdu`, fetches data to be transmitted from RLC and places it in the ULSCH PDU buffer. Inside, it calls `mac_rlc_data_req` to get a data request from RLC.
This is a function in file `nr_rlc_oai_api.c:185`. In this function there is a line: `ret = rb->generate_pdu(rb, buffer_pP, maxsize);` It calls rb (`nr_rlc_entity_t`) to generate a pdu. This function is equal to:

`nr_rlc_entity_am_generate_pdu` function in `openair2/LAYER2/nr_rlc/nr_rlc_entity_am.c:1651`
It calls:

`generate_tx_pdu` function in `openair2/LAYER2/nr_rlc/nr_rlc_entity_am.c:1532`
This function makes a pdu from rlc_entity's queue (tx_list).

Also, it may call `generate_retx_pdu` function `openair2/LAYER2/nr_rlc/nr_rlc_entity_am.c`. It can 