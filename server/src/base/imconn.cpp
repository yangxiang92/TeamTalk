/*
 * imconn.cpp
 *
 *  Created on: 2013-6-5
 *      Author: ziteng@mogujie.com
 */

#include "imconn.h"

//static uint64_t g_send_pkt_cnt = 0;		// 发送数据包总数
//static uint64_t g_recv_pkt_cnt = 0;		// 接收数据包总数

static CImConn* FindImConn(ConnMap_t* imconn_map, net_handle_t handle)
{
	CImConn* pConn = NULL;
	ConnMap_t::iterator iter = imconn_map->find(handle);
	if (iter != imconn_map->end())
	{
		pConn = iter->second;
		pConn->AddRef();
	}

	return pConn;
}

void imconn_callback(void* callback_data, uint8_t msg, uint32_t handle, void* pParam)
{
    // 下面这两个宏定义纯粹是为了消除编译时的warning。。。
	NOTUSED_ARG(handle);
	NOTUSED_ARG(pParam);

    // 为了防止后续发生错误，这里对NULL指针做一个判断
	if (!callback_data)
		return;

    // 一般来说回调函数的数据会是连接的列表，这个列表可能是客户端的，也可能是msg_server的。
	ConnMap_t* conn_map = (ConnMap_t*)callback_data;
    // 在当前的连接列表中查找拥有当前回调函数句柄的连接对象。（一般都能找到的吧。。。）
    // 注意，这里面保存的指针都是子类的指针，所以返回回来的时候都是可以进行多态的对象
	CImConn* pConn = FindImConn(conn_map, handle);
	if (!pConn)
		return;

	//log("msg=%d, handle=%d ", msg, handle);

	switch (msg)
	{
    // 这里全部都是多态！！！
    //
	case NETLIB_MSG_CONFIRM:
        // CImConn对象的这个函数是空函数，应该是使用了多态
		pConn->OnConfirm();
		break;
	case NETLIB_MSG_READ:
        // 这个函数虽然被定义成了虚函数的形式，但是好像一般还是用父类（CImConn）的实现
		pConn->OnRead();
		break;
	case NETLIB_MSG_WRITE:
        // 这个函数好像一般也是用父类（CImConn）的实现
		pConn->OnWrite();
		break;
	case NETLIB_MSG_CLOSE:
        // 又是一个没有实现的虚函数
		pConn->OnClose();
		break;
	default:
		log("!!!imconn_callback error msg: %d ", msg);
		break;
	}

	pConn->ReleaseRef();
}

//////////////////////////
CImConn::CImConn()
{
	//log("CImConn::CImConn ");

	m_busy = false;
	m_handle = NETLIB_INVALID_HANDLE;
	m_recv_bytes = 0;

	m_last_send_tick = m_last_recv_tick = get_tick_count();
}

CImConn::~CImConn()
{
	//log("CImConn::~CImConn, handle=%d ", m_handle);
}

int CImConn::Send(void* data, int len)
{
	m_last_send_tick = get_tick_count();
//	++g_send_pkt_cnt;

	if (m_busy)
	{
		m_out_buf.Write(data, len);
		return len;
	}

	int offset = 0;
	int remain = len;
	while (remain > 0) {
		int send_size = remain;
		if (send_size > NETLIB_MAX_SOCKET_BUF_SIZE) {
			send_size = NETLIB_MAX_SOCKET_BUF_SIZE;
		}

		int ret = netlib_send(m_handle, (char*)data + offset , send_size);
		if (ret <= 0) {
			ret = 0;
			break;
		}

		offset += ret;
		remain -= ret;
	}

	if (remain > 0)
	{
		m_out_buf.Write((char*)data + offset, remain);
		m_busy = true;
		log("send busy, remain=%d ", m_out_buf.GetWriteOffset());
	}
    else
    {
        OnWriteCompelete();
    }

	return len;
}

// 有时候子类不需要重载这个函数，因为读的话都是直接读入到buffer里面，是没有差别的
void CImConn::OnRead()
{
	for (;;)
	{
		uint32_t free_buf_len = m_in_buf.GetAllocSize() - m_in_buf.GetWriteOffset();
		if (free_buf_len < READ_BUF_SIZE)
			m_in_buf.Extend(READ_BUF_SIZE);

		int ret = netlib_recv(m_handle, m_in_buf.GetBuffer() + m_in_buf.GetWriteOffset(), READ_BUF_SIZE);
        // 当没有接收到数据的时候跳出循环
		if (ret <= 0)
			break;

		m_recv_bytes += ret;
		m_in_buf.IncWriteOffset(ret);

		m_last_recv_tick = get_tick_count();
	}

    CImPdu* pPdu = NULL;
	try
    {
        // 当PDU还没有准备好的时候下面这个函数会一致返回NULL
		while ( ( pPdu = CImPdu::ReadPdu(m_in_buf.GetBuffer(), m_in_buf.GetWriteOffset()) ) )
		{
            uint32_t pdu_len = pPdu->GetLength();

            // 也是定义成了虚函数：多态
            // 主要由子类去实现这个里面的功能
			HandlePdu(pPdu);

            // 将PDU的Buffer的偏置指针置为0
            // 但是这种写法真的好么。。。
			m_in_buf.Read(NULL, pdu_len);
            // 处理完了之后就可以删除PDU啦~
			delete pPdu;
            pPdu = NULL;
//			++g_recv_pkt_cnt;
		}
	} catch (CPduException& ex) {
		log("!!!catch exception, sid=%u, cid=%u, err_code=%u, err_msg=%s, close the connection ",
				ex.GetServiceId(), ex.GetCommandId(), ex.GetErrorCode(), ex.GetErrorMsg());
        if (pPdu) {
            delete pPdu;
            pPdu = NULL;
        }
        // 出错了之后记得关掉连接
        OnClose();
	}
}

// 这个部分子类一般也不需要重载，因为直接是把Buffer里面的数据发出去而已
void CImConn::OnWrite()
{
	if (!m_busy)
		return;

	while (m_out_buf.GetWriteOffset() > 0) {
		int send_size = m_out_buf.GetWriteOffset();
        // 尽量发送Buf内所有的字节，
        // 如果比网络库规定的最大的发送大小还要大的话就限制住它
		if (send_size > NETLIB_MAX_SOCKET_BUF_SIZE) {
			send_size = NETLIB_MAX_SOCKET_BUF_SIZE;
		}

        // 返回的是发送出去的字节数
		int ret = netlib_send(m_handle, m_out_buf.GetBuffer(), send_size);
        // 如果没有发送出去，就退出循环
        // 也就是说现在系统处于忙的状态，所以需要退出循环
        // 同时待会还会置这个连接的状态为忙状态
		if (ret <= 0) {
			ret = 0;
			break;
		}

        // 把有效Buf的大小减小
		m_out_buf.Read(NULL, ret);
	}

	if (m_out_buf.GetWriteOffset() == 0) {
        // 如果还有数据没发出去但是又退出了循环，那就是处于忙状态了。
		m_busy = false;
	}

	log("onWrite, remain=%d ", m_out_buf.GetWriteOffset());
}




