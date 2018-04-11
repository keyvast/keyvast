{ KeyVast - A key value store }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/02/07  0.01  Initial development }
{                   System file, Database list file }
{ 2018/02/08  0.02  Dataset list file }
{ 2018/02/09  0.03  Hash file }
{ 2018/02/10  0.04  Blob file }
{ 2018/03/03  0.05  Folders support in hash file }
{ 2018/03/08  0.06  Decrease level slot count from 64 to 32 }
{                   Change hash file record structure for longer keys }
{ 2018/04/08  0.07  Reorganise hash file record structure (breaks
{                   compatibility) and add Timestamp field }
{ 2018/04/11  0.08  Change hash record and blob record to handle 64 bit indexes }
{                   (breaks compatibility) and reserve for 64 bit size }

{$INCLUDE kvInclude.inc}

unit kvStructures;

interface

uses
  SysUtils;



type
  Word16 = Word;
  Word32 = FixedUInt;
  Word64 = UInt64;

  EkvStructure = class(Exception);



{                                                                              }
{ System file                                                                  }
{                                                                              }
{   +=======================+                                                  }
{   | Header                |                                                  }
{   +=======================+                                                  }
{                                                                              }

const
  KV_SystemFileHeader_Magic   = $A55F2101;
  KV_SystemFileHeader_Version = 1;

  KV_SystemName_MaxLength         = 64;
  KV_SystemFile_MaxUserDataLength = 128;

type
  TkvSystemFileHeader = packed record
    Magic             : Word32;        // KV_SystemFileHeader_Magic
    Version           : Word32;        // KV_SystemFileHeader_Version
    HeaderSize        : Word32;        // KV_SystemFile_HeaderSize
    NameLength        : Word32;
    Name              : array[0..KV_SystemName_MaxLength - 1] of WideChar;
    CreationTime      : TDateTime;
    LastOpenTime      : TDateTime;
    UniqueIdCounter   : UInt64;
    UserDataStrLength : Word32;        // User defined data
    UserDataStr       : array[0..KV_SystemFile_MaxUserDataLength - 1] of WideChar;
    Reserved          : array[0..595] of Byte;
  end;
  PkvSystemFileHeader = ^TkvSystemFileHeader;

const
  KV_SystemFile_HeaderSize = SizeOf(TkvSystemFileHeader);
  KV_SystemFile_MinHeaderSize = KV_SystemFile_HeaderSize;

procedure kvInitSystemFileHeader(
          out Header: TkvSystemFileHeader;
          const Name: String;
          const UserData: String);

procedure kvSystemFileHeaderSetUserData(
          var Header: TkvSystemFileHeader;
          const UserData: String);

function  kvSystemFileHeaderGetUserData(
          const Header: TkvSystemFileHeader): String;



{                                                                              }
{ Database list file                                                           }
{                                                                              }
{   +=======================+                                                  }
{   | Header                |                                                  }
{   +=======================+                                                  }
{   | Record                |                                                  }
{   +-----------------------+                                                  }
{   | Record                |                                                  }
{   +-----------------------+                                                  }
{   .                       .                                                  }
{   .                       .                                                  }
{   +=======================+                                                  }
{                                                                              }

const
  KV_DatabaseListFile_Magic   = $A55F2102;
  KV_DatabaseListFile_Version = 1;

type
  TkvDatabaseListFileHeader = packed record
    Magic       : Word32;
    Version     : Word32;
    HeaderSize  : Word32;
    RecordCount : Word32;
    Reserved    : array[0..1007] of Byte;
  end;
  PkvDatabaseListFileHeader = ^TkvDatabaseListFileHeader;

const
  KV_DatabaseListFile_HeaderSize = SizeOf(TkvDatabaseListFileHeader);

procedure kvInitDatabaseListFileHeader(
          out Header: TkvDatabaseListFileHeader);



{ Database list file record }

const
  KV_DatabaseListFileRecord_Magic   = $A55F2103;
  KV_DatabaseListFileRecord_Version = 1;
  KV_DatabaseName_MaxLength         = 64;

type
  TkvDatabaseListFileRecordFlags = set of (
      dblfrfDeleted
    );
  TkvDatabaseListFileRecord = packed record
    Magic           : Word32;
    Version         : Word32;
    Flags           : TkvDatabaseListFileRecordFlags;
    NameLength      : Word32;
    Name            : array[0..KV_DatabaseName_MaxLength - 1] of WideChar;
    CreationTime    : TDateTime;
    UniqueIdCounter : UInt64;
    Reserved        : array[0..866] of Byte;
  end;

const
  KV_DatabaseListFile_RecordSize = SizeOf(TkvDatabaseListFileRecord);

procedure kvInitDatabaseListFileRecord(
          out Rec: TkvDatabaseListFileRecord;
          const Name: String);



{                                                                              }
{ Dataset list file                                                            }
{                                                                              }
{   +=======================+                                                  }
{   | Header                |                                                  }
{   +=======================+                                                  }
{   | Record                |                                                  }
{   +-----------------------+                                                  }
{   | Record                |                                                  }
{   +-----------------------+                                                  }
{   .                       .                                                  }
{   .                       .                                                  }
{   +=======================+                                                  }
{                                                                              }


{ Datset list file header }

const
  KV_DatasetListFileHeader_Magic   = $A55F2104;
  KV_DatasetListFileHeader_Version = 1;

type
  TkvDatasetListFileHeader = packed record
    Magic       : Word32;
    Version     : Word32;
    HeaderSize  : Word32;
    RecordCount : Word32;
    Reserved    : array[0..1007] of Byte;
  end;
  PkvDatasetListFileHeader = ^TkvDatasetListFileHeader;

const
  KV_DatasetListFile_HeaderSize = SizeOf(TkvDatasetListFileHeader);

procedure kvInitDatasetListFileHeader(
          out Header: TkvDatasetListFileHeader);



{ Dataset list file record }

const
  KV_DatasetListFileRecord_Magic   = $A55F2105;
  KV_DatasetListFileRecord_Version = 1;
  KV_DatasetName_MaxLength         = 64;

type
  TkvDatasetListFileRecordFlags = set of (
      dslfrfDeleted
    );
  TkvDatasetListFileRecord = packed record
    Magic        : Word32;
    Version      : Word32;
    Flags        : TkvDatasetListFileRecordFlags;
    NameLength   : Word32;
    Name         : array[0..KV_DatasetName_MaxLength - 1] of WideChar;
    CreationTime : TDateTime;
    Reserved     : array[0..874] of Byte;
  end;

const
  KV_DatasetListFile_RecordSize = SizeOf(TkvDatasetListFileRecord);

procedure kvInitDatasetListFileRecord(
          var Rec: TkvDatasetListFileRecord;
          const Name: String);



{                                                                              }
{ Hash file                                                                    }
{                                                                              }
{   +=======================+                                                  }
{   | Header                |                                                  }
{   +=======================+                                                  }
{   | 64 Slot records       |  Level 1 slots                                   }
{   +-----------------------+                                                  }
{   | 64 Slot records       |  One of level 2+ slots                           }
{   +-----------------------+                                                  }
{   .                       .                                                  }
{   .                       .                                                  }
{   +=======================+                                                  }
{                                                                              }
{ Slot record                                                                  }
{                                                                              }
{   +--------------------------------------+                                   }
{   | <Empty>      or                      |                                   }
{   | Key/Value    or                      |                                   }
{   | Pointer to next level 64 x Record    |                                   }
{   +--------------------------------------+                                   }
{                                                                              }
{ 64 Slot records example:                                                     }
{                                                                              }
{   +-------------+                                                            }
{   | Key/Value   |                                                            }
{   +-------------+                                                            }
{   | <Empty>     |                                                            }
{   +-------------+                                                            }
{   | Next level  |-----> +-------------+                                      }
{   +-------------+       | Key/Value   |                                      |
{   .             .       +-------------+                                      }
{   .             .       | Next level  |-----> +-------------+                }
{   +-------------+       +-------------+       .             .                }
{                         .             .       .             .                }
{                         .             .       +-------------+                }
{                         +-------------+                                      }
{                                                                              }

const
  KV_HashFile_LevelSlotCount = 32;
  KV_HashFile_InvalidIndex   = $FFFFFFFFFFFFFFFF;
  KV_HashFile_MaxKeyLength   = $FFFF;



{ Hash file header }

const
  KV_HashFileHeader_Magic   = $A55F2106;
  KV_HashFileHeader_Version = 1;

type
  TkvHashFileHeader = packed record
    Magic             : Word32;        // KV_HashFileHeader_Magic
    Version           : Word32;        // KV_HashFileHeader_Version
    HeaderSize        : Word32;        // KV_HashFile_HeaderSize
    LevelSlotCount    : Word32;        // Slots count per level (fixed)
    UniqueIdCounter   : UInt64;
    RecordCount       : Word64;
    FirstDeletedIndex : Word64;        // Linked list of deleted records
    Reserved          : array[0..983] of Byte;
  end;
  PkvHashFileHeader = ^TkvHashFileHeader;

const
  KV_HashFile_HeaderSize = SizeOf(TkvHashFileHeader);

procedure kvInitHashFileHeader(
          out Header: TkvHashFileHeader);



{ Hash file record }

const
  KV_HashFileRecord_Magic   = $A507;
  KV_HashFileRecord_Version = 1;

  KV_HashFileRecord_SlotShortKeyLength = 26;
  KV_HashFileRecord_SlotShortValueSize = 28;

type
  TkvHashFileRecordType = (
      hfrtEmpty,
      hfrtParentSlot,
      hfrtKeyValue,
      hfrtKeyValueWithHashCollision,
      hfrtDeleted
    );
  TkvHashFileRecordValueType = (
      hfrvtNone,
      hfrvtShort,
      hfrvtLong,
      hfrvtFolder
    );
  TkvHashFileRecord = packed record
    Magic                 : Word16;                 // KV_HashFileRecord_Magic
    Version               : Byte;                   // KV_HashFileRecord_Version
    RecordType            : TkvHashFileRecordType;
    Timestamp             : TDateTime;              // Timestamp of last change for Key/Value record types
    ChildSlotRecordIndex  : Word64;                 // Used by ParentSlot and HashCollision
    KeyHash               : UInt64;                 // kvLevelHash of Key
    KeyLength             : Word16;
    KeyShort              : array[0..KV_HashFileRecord_SlotShortKeyLength - 1] of WideChar;
    KeyLongChainIndex     : Word64;
    ValueType             : TkvHashFileRecordValueType;
    ValueTypeId           : Byte;
    ValueSize             : Word32;
    ValueSize_Reserved    : Word32;
    case Integer of
      0 : (ValueShort           : array[0..KV_HashFileRecord_SlotShortValueSize - 1] of Byte);
      1 : (ValueLongChainIndex  : Word64);
      2 : (ValueFolderBaseIndex : Word64);
  end;

const
  KV_HashFile_RecordSize = SizeOf(TkvHashFileRecord); // 128 bytes

procedure kvInitHashFileRecord(
          out Rec: TkvHashFileRecord);



{                                                                              }
{ Blob file                                                                    }
{                                                                              }
{   +=======================+                                                  }
{   | Header                |                                                  }
{   | FreeRecordPtr         | ----> <Rec> ----> <Rec> ...                      }
{   +=======================+                                                  }
{   | Record                |                                                  }
{   | NextRecordPtr         | ----> <Rec> ----> <Rec> ...                      }
{   +-----------------------+                                                  }
{   .                       .                                                  }
{   .                       .                                                  }
{   +=======================+                                                  }
{                                                                              }

const
  KV_BlobFile_MinRecordSize      = 128;
  KV_BlobFile_RecordSizeMultiple = 128;
  KV_BlobFile_InvalidIndex       = Word64($FFFFFFFFFFFFFFFF);



{ Blob file header }

const
  KV_BlobFileHeader_Magic   = $A55F2108;
  KV_BlobFileHeader_Version = 1;

type
  TkvBlobFileHeader = packed record
    Magic           : Word32;        // KV_BlobFileHeader_Magic
    Version         : Word32;        // KV_BlobFileHeader_Version
    HeaderSize      : Word32;        // KV_BlobFile_HeaderSize
    RecordSize      : Word32;
    RecordCount     : Word64;
    FreeRecordIndex : Word64;        // Linked list to free records
    FreeRecordCount : Word64;
    Reserved        : array[0..983] of Byte;
  end;
  PkvBlobFileHeader = ^TkvBlobFileHeader;

const
  KV_BlobFile_HeaderSize = SizeOf(TkvBlobFileHeader);

procedure kvInitBlobFileHeader(
          out Header: TkvBlobFileHeader;
          const RecordSize: Word32);



{ Blob file record }

const
  KV_BlobFileRecordHeader_Magic   = $A509;
  KV_BlobFileRecordHeader_Version = 1;

  KV_BlobFile_MaxChainSize = $7FFFFFFF;

type
  TkvBlobFileRecordHeader = packed record
    Magic           : Word;        // KV_BlobFileRecordHeader_Magic
    Version         : Word;        // KV_BlobFileRecordHeader_Version
    NextRecordIndex : Word64;      // Next record in chain
    case Integer of
      // Only used by first record header in a chain
      0 : (LastRecordIndex : Word64;
           ChainSize       : Word32);
      1 : (Reserved        : array[0..11] of Byte);
  end;
  PkvBlobFileRecordHeader = ^TkvBlobFileRecordHeader;

const
  KV_BlobFile_RecordHeaderSize = SizeOf(TkvBlobFileRecordHeader); // 24 bytes

procedure kvInitBlobFileRecordHeader(
          out Header: TkvBlobFileRecordHeader);



implementation



{ System file header }

procedure kvInitSystemFileHeader(out Header: TkvSystemFileHeader;
          const Name: String;
          const UserData: String);
var
  L : Integer;
begin
  FillChar(Header, SizeOf(Header), 0);

  Header.Magic := KV_SystemFileHeader_Magic;
  Header.Version := KV_SystemFileHeader_Version;
  Header.HeaderSize := KV_SystemFile_HeaderSize;

  L := Length(Name);
  if L > KV_SystemName_MaxLength then
    raise EkvStructure.CreateFmt('System name too long: %s', [Name]);
  Header.NameLength := L;
  if L > 0 then
    Move(PChar(Name)^, Header.Name[0], L * SizeOf(WideChar));

  Header.CreationTime := Now;

  kvSystemFileHeaderSetUserData(Header, UserData);
end;

procedure kvSystemFileHeaderSetUserData(var Header: TkvSystemFileHeader;
          const UserData: String);
var
  L : Integer;
begin
  L := Length(UserData);
  if L > KV_SystemFile_MaxUserDataLength then
    raise EkvStructure.Create('System user data too long');
  Header.UserDataStrLength := L;
  if L > 0 then
    Move(PChar(UserData)^, Header.UserDataStr[0], L * SizeOf(WideChar));
end;

function kvSystemFileHeaderGetUserData(const Header: TkvSystemFileHeader): String;
var
  L : Integer;
  S : String;
begin
  L := Header.UserDataStrLength;
  Assert(L <= KV_SystemFile_MaxUserDataLength);
  SetLength(S, L);
  if L > 0 then
    Move(Header.UserDataStr[0], PChar(S)^, L * SizeOf(WideChar));
  Result := S;
end;



{ Database list file header }

procedure kvInitDatabaseListFileHeader(out Header: TkvDatabaseListFileHeader);
begin
  FillChar(Header, SizeOf(Header), 0);
  Header.Magic := KV_DatabaseListFile_Magic;
  Header.Version := KV_DatabaseListFile_Version;
  Header.HeaderSize := KV_DatabaseListFile_HeaderSize;
end;



{ Database list file record }

procedure kvInitDatabaseListFileRecord(out Rec: TkvDatabaseListFileRecord;
          const Name: String);
var
  L : Integer;
begin
  FillChar(Rec, SizeOf(Rec), 0);
  Rec.Magic := KV_DatabaseListFileRecord_Magic;
  Rec.Version := KV_DatabaseListFileRecord_Version;
  Rec.Flags := [];

  L := Length(Name);
  Rec.NameLength := L;
  if L > KV_DatabaseName_MaxLength then
    raise EkvStructure.CreateFmt('Database name too long: %s', [Name]);
  if Rec.NameLength > 0 then
    Move(PChar(Name)^, Rec.Name[0], L * SizeOf(Char));

  Rec.CreationTime := Now;
end;



{ Dataset list file header }

procedure kvInitDatasetListFileHeader(out Header: TkvDatasetListFileHeader);
begin
  FillChar(Header, SizeOf(Header), 0);
  Header.Magic := KV_DatasetListFileHeader_Magic;
  Header.Version := KV_DatasetListFileHeader_Version;
  Header.HeaderSize := KV_DatasetListFile_HeaderSize;
end;



{ Dataset list file record }

procedure kvInitDatasetListFileRecord(var Rec: TkvDatasetListFileRecord;
          const Name: String);
var
  L : Integer;
begin
  FillChar(Rec, SizeOf(Rec), 0);
  Rec.Magic := KV_DatasetListFileRecord_Magic;
  Rec.Version := KV_DatasetListFileRecord_Version;

  L := Length(Name);
  Rec.NameLength := L;
  if L > KV_DatasetName_MaxLength then
    raise EkvStructure.CreateFmt('Dataset name too long: %s', [Name]);
  if Rec.NameLength > 0 then
    Move(PChar(Name)^, Rec.Name[0], L * SizeOf(Char));

  Rec.CreationTime := Now;
end;



{ Hash file header }

procedure kvInitHashFileHeader(out Header: TkvHashFileHeader);
begin
  FillChar(Header, SizeOf(Header), 0);
  Header.Magic := KV_HashFileHeader_Magic;
  Header.Version := KV_HashFileHeader_Version;
  Header.HeaderSize := KV_HashFile_HeaderSize;
  Header.LevelSlotCount := KV_HashFile_LevelSlotCount;
  Header.FirstDeletedIndex := KV_HashFile_InvalidIndex;
end;



{ Hash file record }

procedure kvInitHashFileRecord(out Rec: TkvHashFileRecord);
begin
  FillChar(Rec, SizeOf(Rec), 0);
  Rec.Magic := KV_HashFileRecord_Magic;
  Rec.Version := KV_HashFileRecord_Version;
  Rec.RecordType := hfrtEmpty;
  Rec.ChildSlotRecordIndex := KV_HashFile_InvalidIndex;
  Rec.KeyLongChainIndex := KV_BlobFile_InvalidIndex;
  Rec.ValueType := hfrvtNone;
  Rec.ValueLongChainIndex := KV_BlobFile_InvalidIndex;
end;



{ Blob file header }

procedure kvInitBlobFileHeader(out Header: TkvBlobFileHeader;
          const RecordSize: Word32);
begin
  if (RecordSize < KV_BlobFile_MinRecordSize) or
     (RecordSize mod KV_BlobFile_RecordSizeMultiple <> 0) then
    raise EkvStructure.CreateFmt('Invalid record size: %d', [RecordSize]);
  FillChar(Header, SizeOf(Header), 0);
  Header.Magic := KV_BlobFileHeader_Magic;
  Header.Version := KV_BlobFileHeader_Version;
  Header.HeaderSize := KV_BlobFile_HeaderSize;
  Header.RecordSize := RecordSize;
  Header.FreeRecordIndex := KV_BlobFile_InvalidIndex;
end;



{ Blob file record }

procedure kvInitBlobFileRecordHeader(out Header: TkvBlobFileRecordHeader);
begin
  FillChar(Header, SizeOf(Header), 0);
  Header.Magic := KV_BlobFileRecordHeader_Magic;
  Header.Version := KV_BlobFileRecordHeader_Version;
  Header.NextRecordIndex := KV_BlobFile_InvalidIndex;
  Header.LastRecordIndex := KV_BlobFile_InvalidIndex;
end;



end.

