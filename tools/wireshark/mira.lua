-- MIRA-T/1 revision 4 Wireshark UDP dissector. Only displays the plaintext
-- header; application frames are AEAD ciphertext and cannot be decoded here.
-- Configure UDP port 7447 or update the registration below.
local p=Proto("mirat","MIRA-T/1 experimental UDP")
local magic=ProtoField.uint16("mirat.magic","Magic",base.HEX)
local version=ProtoField.uint8("mirat.version","Wire Version",base.DEC)
local kind=ProtoField.uint8("mirat.kind","Packet Kind",base.DEC)
local flags=ProtoField.uint8("mirat.flags","Flags",base.HEX)
local length=ProtoField.uint16("mirat.length","Encrypted Body Bytes",base.DEC)
local cid=ProtoField.uint64("mirat.cid","Connection ID",base.HEX)
local pn=ProtoField.uint64("mirat.packet_number","Packet Number",base.DEC)
local epoch=ProtoField.uint16("mirat.key_epoch","Key Epoch",base.DEC)
p.fields={magic,version,kind,flags,cid,pn,epoch,length}
function p.dissector(buf,pinfo,tree)
 if buf:len()<26 or buf(0,2):uint()~=0x4D54 or buf(2,1):uint()~=4 or buf(5,1):uint()~=26 then return 0 end
 local size=buf(24,2):uint()
 if size+26~=buf:len() then return 0 end
 pinfo.cols.protocol="MIRA-T"
 local n=tree:add(p,buf())
 n:add(magic,buf(0,2));n:add(version,buf(2,1));n:add(kind,buf(3,1));n:add(flags,buf(4,1))
 n:add(cid,buf(6,8));n:add(pn,buf(14,8));n:add(epoch,buf(22,2));n:add(length,buf(24,2))
 n:add(buf(26,size),"Opaque handshake body / authenticated ciphertext")
 return buf:len()
end
DissectorTable.get("udp.port"):add(7447,p)
