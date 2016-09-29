/*
 * ImPduBase.cpp
 *
 *  Created on: 2013-8-27
 *      Author: ziteng@mogujie.com
 */

#include "util.h"
#include "ImPduBase.h"
#include "IM.BaseDefine.pb.h"
using namespace IM::BaseDefine;

CImPdu::CImPdu()
{
	m_pdu_header.version = IM_PDU_VERSION;
	m_pdu_header.flag = 0;
	m_pdu_header.service_id = SID_OTHER;
	m_pdu_header.command_id = 0;
	m_pdu_header.seq_num = 0;
    m_pdu_header.reversed = 0;
}

uchar_t* CImPdu::GetBuffer()
{
    return m_buf.GetBuffer();
}

uint32_t CImPdu::GetLength()
{
    // PDU的长度实际上就是Buffer的有效区域的长度
    return m_buf.GetWriteOffset();
}

uchar_t* CImPdu::GetBodyData()
{
    return m_buf.GetBuffer() + sizeof(PduHeader_t);
}

uint32_t CImPdu::GetBodyLength()
{
    uint32_t body_length = 0;
    body_length = m_buf.GetWriteOffset() - sizeof(PduHeader_t);
    return body_length;
}

void CImPdu::WriteHeader()
{
	uchar_t* buf = GetBuffer();

	CByteStream::WriteInt32(buf, GetLength());
	CByteStream::WriteUint16(buf + 4, m_pdu_header.version);
	CByteStream::WriteUint16(buf + 6, m_pdu_header.flag);
	CByteStream::WriteUint16(buf + 8, m_pdu_header.service_id);
	CByteStream::WriteUint16(buf + 10, m_pdu_header.command_id);
    CByteStream::WriteUint16(buf + 12, m_pdu_header.seq_num);
    CByteStream::WriteUint16(buf + 14, m_pdu_header.reversed);
}

void CImPdu::SetVersion(uint16_t version)
{
	uchar_t* buf = GetBuffer();
	CByteStream::WriteUint16(buf + 4, version);
}

void CImPdu::SetFlag(uint16_t flag)
{
	uchar_t* buf = GetBuffer();
	CByteStream::WriteUint16(buf + 6, flag);
}

void CImPdu::SetServiceId(uint16_t service_id)
{
    uchar_t* buf = GetBuffer();
    // +8 是为了干什么？？？
    CByteStream::WriteUint16(buf + 8, service_id);
}

void CImPdu::SetCommandId(uint16_t command_id)
{
    uchar_t* buf = GetBuffer();
    CByteStream::WriteUint16(buf + 10, command_id);
}

void CImPdu::SetError(uint16_t error)
{
    uchar_t* buf = GetBuffer();
    CByteStream::WriteUint16(buf + 12, error);
}

void CImPdu::SetSeqNum(uint16_t seq_num)
{
	uchar_t* buf = GetBuffer();
	CByteStream::WriteUint16(buf + 12, seq_num);
}

void CImPdu::SetReversed(uint32_t reversed)
{
    uchar_t* buf = GetBuffer();
    CByteStream::WriteUint16(buf+14, reversed);
}

int CImPdu::ReadPduHeader(uchar_t* buf, uint32_t len)
{
	int ret = -1;
	if (len >= IM_PDU_HEADER_LEN && buf) {
        // 字节流
		CByteStream is(buf, len);

		is >> m_pdu_header.length;
		is >> m_pdu_header.version;
		is >> m_pdu_header.flag;
		is >> m_pdu_header.service_id;
		is >> m_pdu_header.command_id;
		is >> m_pdu_header.seq_num;
        is >> m_pdu_header.reversed;

		ret = 0;
	}

	return ret;
}

CImPdu* CImPdu::ReadPdu(uchar_t *buf, uint32_t len)
{
	uint32_t pdu_len = 0;
    // 检查PDU是否就绪
	if (!IsPduAvailable(buf, len, pdu_len))
		return NULL;

    // 偏置为8的地方是service_id？？？
    // 我擦。。。这是自己随便写的吧。。。
	uint16_t service_id = CByteStream::ReadUint16(buf + 8);
    // 偏执为10的地方是command_id
    // 这应该也是自己随便写的吧。。。
	uint16_t command_id = CByteStream::ReadUint16(buf + 10);
	CImPdu* pPdu = NULL;

    pPdu = new CImPdu();
    //pPdu->_SetIncomingLen(pdu_len);
    //pPdu->_SetIncomingBuf(buf);
    // 往PDU的Buffer里面写数据
    pPdu->Write(buf, pdu_len);
    // PDU的Buffer里面就包括了PDU的Header
    // 从buf开始偏置为16的地址就放着PDU的Header
    pPdu->ReadPduHeader(buf, IM_PDU_HEADER_LEN);

    // 将解析好的PDU返回
    return pPdu;
}

bool CImPdu::IsPduAvailable(uchar_t* buf, uint32_t len, uint32_t& pdu_len)
{
	if (len < IM_PDU_HEADER_LEN)
		return false;

    // 读取网络字节流的第一个字节（注意网络字节流是大端存储的）
    // 这个字节就是PDU的长度
	pdu_len = CByteStream::ReadUint32(buf);
	if (pdu_len > len)
	{
        // PDU的长度比Buffer的长度还长的话，显然是不行的，还需要继续接受数据才行
		//log("pdu_len=%d, len=%d\n", pdu_len, len);
		return false;
	}

    // 如果PDU的长度为0，那么就抛出异常
    if(0 == pdu_len)
    {
        throw CPduException(1, "pdu_len is 0");
    }

    // 如果一切准备就绪，则可以开出处理PDU
	return true;
}

void CImPdu::SetPBMsg(const google::protobuf::MessageLite* msg)
{
    //设置包体，则需要重置下空间
    m_buf.Read(NULL, m_buf.GetWriteOffset());
    m_buf.Write(NULL, sizeof(PduHeader_t));
    uint32_t msg_size = msg->ByteSize();
    uchar_t* szData = new uchar_t[msg_size];
    //ALLOC_FAIL_ASSERT(szData)
    if (!msg->SerializeToArray(szData, msg_size))
    {
        log("pb msg miss required fields.");
    }

    m_buf.Write(szData, msg_size);
    delete []szData;
    WriteHeader();
}

