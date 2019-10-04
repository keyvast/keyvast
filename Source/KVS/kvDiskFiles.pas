{ KeyVast - A key value store }
{ Copyright (c) 2018-2019 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/02/07  0.01  Initial development }
{                   System file, Database list file }
{ 2018/02/08  0.02  Dataset list file }
{ 2018/02/09  0.03  Hash file }
{ 2018/02/10  0.04  Blob file }
{ 2018/02/18  0.05  Hash file cache }
{ 2018/03/05  0.06  Blob file append chain }
{ 2018/03/14  0.07  Blob file truncate }
{ 2018/04/11  0.08  Blob file 64-bit indexes }
{ 2018/04/11  0.09  Hash file 64-bit indexes }
{ 2019/04/19  0.10  Initialise header before CreateFile in OpenNew }
{ 2019/09/11  0.11  WriteChain/TruncateChainAt add records to free list when unused }
{ 2019/09/30  0.12  Rename to kvDiskFiles }
{ 2019/10/04  0.13  Increase Hash file default cache entries }
{ 2019/10/04  0.14  Hash file Timestamp functions }

{$INCLUDE kvInclude.inc}

unit kvDiskFiles;

interface

uses
  {$IFDEF POSIX}
  Posix.Unistd,
  {$ENDIF}
  SysUtils,
  Classes,

  kvDiskFileStructures;



type
  EkvFile = class(Exception);



{ AkvFile }

type
  AkvFile = class
  private
    FFile : TFileStream;

  protected
    procedure LogWarning(const Txt: String);

  public
    destructor Destroy; override;
    procedure Finalise; virtual;
  end;



{ TkvSystemFile }

type
  TkvSystemFile = class(AkvFile)
  private
    FPath       : String;
    FSystemName : String;

    FFileName   : String;
    FFileHeader : TkvSystemFileHeader;

    procedure CreateFile;
    procedure OpenFile;
    procedure CloseFile;
    procedure InitHeader(const UserDataStr: String);
    procedure LoadHeader;
    procedure SaveHeader;

    function  GetUserDataStr: String;
    procedure SetUserDataStr(const A: String);

  public
    constructor Create(const Path, SystemName: String);

    property  FileName: String read FFileName;

    function  Exists: Boolean;
    procedure OpenNew(const UserDataStr: String);
    procedure Open;
    procedure Close;
    procedure Delete;

    function  GetHeader: PkvSystemFileHeader;
    procedure HeaderModified;

    property  UserDataStr: String read GetUserDataStr write SetUserDataStr;
    function  AllocateUniqueId: UInt64;
  end;



{ TkvDatabaseListFile }

type
  TkvDatabaseListFile = class(AkvFile)
  private
    FPath       : String;
    FSystemName : String;

    FFileName   : String;
    FFileHeader : TkvDatabaseListFileHeader;

    procedure CreateFile;
    procedure OpenFile;
    procedure CloseFile;
    procedure InitHeader;
    procedure LoadHeader;
    procedure SaveHeader;
    procedure SeekRecord(const Idx: Word32);

  public
    constructor Create(const Path, SystemName: String);

    procedure OpenNew;
    procedure Open;
    procedure Close;
    procedure Delete;

    function  GetHeader: PkvDatabaseListFileHeader;
    procedure HeaderModified;

    function  GetRecordCount: Integer;
    procedure LoadRecord(const Idx: Word32; var Rec: TkvDatabaseListFileRecord);
    procedure SaveRecord(const Idx: Word32; const Rec: TkvDatabaseListFileRecord);
    function  AppendRecord(const Rec: TkvDatabaseListFileRecord): Word32;
  end;



{ TkvDatasetListFile }

type
  TkvDatasetListFile = class(AkvFile)
  private
    FPath         : String;
    FSystemName   : String;
    FDatabaseName : String;

    FFileName   : String;
    FFileHeader : TkvDatasetListFileHeader;

    procedure CreateFile;
    procedure OpenFile;
    procedure CloseFile;
    procedure InitHeader;
    procedure LoadHeader;
    procedure SaveHeader;
    procedure SeekRecord(const Idx: Word32);

  public
    constructor Create(const Path, SystemName, DatabaseName: String);

    procedure OpenNew;
    procedure Open;
    procedure Close;
    procedure Delete;

    function  GetHeader: PkvDatasetListFileHeader;
    procedure HeaderModified;

    function  GetRecordCount: Integer;
    procedure LoadRecord(const Idx: Word32; var Rec: TkvDatasetListFileRecord);
    procedure SaveRecord(const Idx: Word32; const Rec: TkvDatasetListFileRecord);
    function  AppendRecord(const Rec: TkvDatasetListFileRecord): Word32;
  end;



{ TkvHashFile }

const
  KV_HashFile_MinCacheEntries     = 0;
  KV_HashFile_MaxCacheEntries     = 262144;
  KV_HashFile_DefaultCacheEntries = KV_HashFile_LevelSlotCount *
                                    KV_HashFile_LevelSlotCount *
                                    KV_HashFile_LevelSlotCount; // 3 levels, 32768 entries, 4 MB

type
  TkvHashFile = class(AkvFile)
  private
    FPath         : String;
    FSystemName   : String;
    FDatabaseName : String;
    FDatasetName  : String;

    FFileName       : String;
    FFileHeader     : TkvHashFileHeader;
    FHeaderModified : Boolean;
    FCacheEntries   : Word32;
    FCacheRec       : array of TkvHashFileRecord;
    FCacheValid     : array of Boolean;

    procedure CreateFile;
    procedure OpenFile;
    procedure CloseFile;
    procedure InitHeader;
    procedure LoadHeader;
    procedure SaveHeader;
    procedure SeekRecord(const Idx: Word64);

  public
    constructor Create(
                const Path, SystemName, DatabaseName, DatasetName: String;
                const CacheEntries: Word32 = KV_HashFile_DefaultCacheEntries);

    procedure OpenNew;
    procedure Open;
    procedure Close;
    procedure Delete;

    function  GetHeader: PkvHashFileHeader;
    procedure HeaderModified;
    procedure UpdateHeader;

    function  AllocateUniqueId: UInt64;
    function  GetNextTimestamp: UInt64;
    procedure UpdateTimestamp(const Timestamp: UInt64);

    function  GetRecordCount: Integer;
    procedure LoadRecord(const Idx: Word64; var Rec: TkvHashFileRecord);
    procedure SaveRecord(const Idx: Word64; const Rec: TkvHashFileRecord);

    function  AllocateSlotRecords: Word64;

    procedure AddDeletedSlots(const BaseIdx: Word64);
  end;



{ TkvBlobFile }

type
  TkvBlobFile = class(AkvFile)
  private
    FPath         : String;
    FSystemName   : String;
    FDatabaseName : String;
    FDatasetName  : String;
    FBlobName     : String;

    FFileName   : String;
    FFileHeader : TkvBlobFileHeader;

    procedure CreateFile;
    procedure OpenFile;
    procedure CloseFile;
    procedure InitHeader(const RecordSize: Word32);
    procedure LoadHeader;
    procedure SaveHeader;
    procedure SeekRecord(const Idx: Word64);
    procedure LoadRecordHeader(const Idx: Word64; var Hdr: TkvBlobFileRecordHeader);
    procedure SaveRecordHeader(const Idx: Word64; const Hdr: TkvBlobFileRecordHeader);
    function  AllocateRecord(var RecHdr: TkvBlobFileRecordHeader): Word64;

  public
    constructor Create(const Path, SystemName, DatabaseName, DatasetName, BlobName: String);

    procedure OpenNew(const RecordSize: Integer);
    procedure Open;
    procedure Close;
    procedure Delete;

    function  GetHeader: PkvBlobFileHeader;
    procedure HeaderModified;

    function  GetRecordSize: Integer;
    function  GetRecordDataSize: Integer;
    function  GetRecordCount: Integer;
    function  GetFreeRecordCount: Integer;

    function  CreateChain(const Buf; const BufSize: Integer): Word64;
    procedure ReadChain(const RecordIndex: Word64; var Buf; const BufSize: Integer);
    procedure ReleaseChain(const RecordIndex: Word64);
    procedure WriteChain(const RecordIndex: Word64; const Buf; const BufSize: Integer);
    function  GetChainSize(const RecordIndex: Word64): Integer;
    procedure AppendChain(const RecordIndex: Word64; const Buf; const BufSize: Integer);
    procedure WriteChainStart(const RecordIndex: Word64; const Buf; const BufSize: Integer);
    procedure TruncateChainAt(const RecordIndex: Word64; const Size: Integer);
  end;



{ Helper functions }

procedure PathEnsureSuffix(var Path: String);



implementation




{ Helper functions }

procedure PathEnsureSuffix(var Path: String);
begin
  Path := Trim(Path);
  if Path = '' then
    exit;
  if Path[Length(Path)] <> PathDelim then
    Path := Path + PathDelim;
end;



{ AkvFile }

destructor AkvFile.Destroy;
begin
  FreeAndNil(FFile);
  inherited Destroy;
end;

procedure AkvFile.Finalise;
begin
end;

procedure AkvFile.LogWarning(const Txt: String);
begin
end;



{ TkvSystemFile }

constructor TkvSystemFile.Create(const Path, SystemName: String);
begin
  if SystemName = '' then
    raise EkvFile.Create('System name required');

  inherited Create;
  FPath := Path;
  FSystemName := SystemName;

  PathEnsureSuffix(FPath);
  FFileName := FPath + FSystemName + '.kvsys';
end;

function TkvSystemFile.Exists: Boolean;
begin
  Result := FileExists(FFileName);
end;

procedure TkvSystemFile.OpenNew(const UserDataStr: String);
begin
  InitHeader(UserDataStr);
  CreateFile;
  FFileHeader.LastOpenTime := Now;
  SaveHeader;
end;

procedure TkvSystemFile.Open;
begin
  OpenFile;
  LoadHeader;
  FFileHeader.LastOpenTime := Now;
  SaveHeader;
end;

procedure TkvSystemFile.Close;
begin
  CloseFile;
end;

procedure TkvSystemFile.Delete;
begin
  DeleteFile(FFileName);
end;

procedure TkvSystemFile.CreateFile;
begin
  if FileExists(FFileName) then
    raise EkvFile.CreateFmt('System exists: %s %s', [FSystemName, FPath]);
  FFile := TFileStream.Create(FFileName, fmCreate or fmShareExclusive);
end;

procedure TkvSystemFile.OpenFile;
begin
  if not FileExists(FFileName) then
    raise EkvFile.CreateFmt('System does not exists: %s %s', [FSystemName, FPath]);
  FFile := TFileStream.Create(FFileName, fmOpenReadWrite or fmShareExclusive);
end;

procedure TkvSystemFile.CloseFile;
begin
  FreeAndNil(FFile);
end;

procedure TkvSystemFile.InitHeader(const UserDataStr: String);
begin
  kvInitSystemFileHeader(FFileHeader, FSystemName, UserDataStr);
end;

procedure TkvSystemFile.LoadHeader;
begin
  Assert(Assigned(FFile));

  if FFile.Size < KV_SystemFile_MinHeaderSize then
    raise EkvFile.Create('System file corrupt: File too small');

  FFile.Position := 0;
  FFile.ReadBuffer(FFileHeader, KV_SystemFile_HeaderSize);

  if (FFileHeader.Magic <> KV_SystemFileHeader_Magic) or
     (FFileHeader.Version = 0) or
     (FFileHeader.HeaderSize <> KV_SystemFile_HeaderSize) then
    raise EkvFile.Create('System file corrupt: Invalid header');
end;

procedure TkvSystemFile.SaveHeader;
begin
  Assert(Assigned(FFile));

  FFile.Position := 0;
  FFile.WriteBuffer(FFileHeader, KV_SystemFile_HeaderSize);
end;

function TkvSystemFile.GetHeader: PkvSystemFileHeader;
begin
  Result := @FFileHeader;
end;

procedure TkvSystemFile.HeaderModified;
begin
  SaveHeader;
end;

function TkvSystemFile.GetUserDataStr: String;
begin
  Result := kvSystemFileHeaderGetUserData(FFileHeader);
end;

procedure TkvSystemFile.SetUserDataStr(const A: String);
begin
  kvSystemFileHeaderSetUserData(FFileHeader, A);
  SaveHeader;
end;

function TkvSystemFile.AllocateUniqueId: UInt64;
var
  A : UInt64;
begin
  A := UInt64(FFileHeader.UniqueIdCounter + 1);
  FFileHeader.UniqueIdCounter := A;
  SaveHeader;
  Result := A;
end;



{ TkvDatabaseListFile }

constructor TkvDatabaseListFile.Create(const Path, SystemName: String);
begin
  if SystemName = '' then
    raise EkvFile.Create('System name required');

  inherited Create;
  FPath := Path;
  FSystemName := SystemName;

  PathEnsureSuffix(FPath);
  FFileName := FPath + FSystemName + '.kvdbl';
end;

procedure TkvDatabaseListFile.OpenNew;
begin
  InitHeader;
  CreateFile;
  SaveHeader;
end;

procedure TkvDatabaseListFile.Open;
begin
  OpenFile;
  LoadHeader;
end;

procedure TkvDatabaseListFile.Close;
begin
  CloseFile;
end;

procedure TkvDatabaseListFile.Delete;
begin
  DeleteFile(FFileName);
end;

procedure TkvDatabaseListFile.CreateFile;
begin
  if FileExists(FFileName) then
    raise EkvFile.CreateFmt('Database list file exists: %s', [FFileName]);
  FFile := TFileStream.Create(FFileName, fmCreate or fmShareExclusive);
end;

procedure TkvDatabaseListFile.OpenFile;
begin
  if not FileExists(FFileName) then
    raise EkvFile.CreateFmt('Database list file does not exists: %s', [FFileName]);
  FFile := TFileStream.Create(FFileName, fmOpenReadWrite or fmShareExclusive);
end;

procedure TkvDatabaseListFile.CloseFile;
begin
  FreeAndNil(FFile);
end;

procedure TkvDatabaseListFile.InitHeader;
begin
  kvInitDatabaseListFileHeader(FFileHeader);
end;

procedure TkvDatabaseListFile.LoadHeader;
var
  FileRecCount : Int64;
begin
  Assert(Assigned(FFile));

  if FFile.Size < KV_DatabaseListFile_HeaderSize then
    raise EkvFile.Create('Database list file corrupt: File too small');

  FFile.Position := 0;
  FFile.ReadBuffer(FFileHeader, KV_DatabaseListFile_HeaderSize);

  if (FFileHeader.Magic <> KV_DatabaseListFile_Magic) or
     (FFileHeader.Version = 0) or
     (FFileHeader.HeaderSize <> KV_DatabaseListFile_HeaderSize)  then
    raise EkvFile.Create('Database list file corrupt: Invalid header');

  FileRecCount := (FFile.Size - KV_DatabaseListFile_HeaderSize) div
                  KV_DatabaseListFile_RecordSize;
  if FileRecCount < FFileHeader.RecordCount then
    raise EkvFile.Create('Database list file corrupt: Missing records');
end;

procedure TkvDatabaseListFile.SaveHeader;
begin
  Assert(Assigned(FFile));

  FFile.Position := 0;
  FFile.WriteBuffer(FFileHeader, KV_DatabaseListFile_HeaderSize);
end;

function TkvDatabaseListFile.GetHeader: PkvDatabaseListFileHeader;
begin
  Result := @FFileHeader;
end;

procedure TkvDatabaseListFile.HeaderModified;
begin
  SaveHeader;
end;

function TkvDatabaseListFile.GetRecordCount: Integer;
begin
  Result := FFileHeader.RecordCount;
end;

procedure TkvDatabaseListFile.SeekRecord(const Idx: Word32);
begin
  Assert(Assigned(FFile));

  if Idx >= FFileHeader.RecordCount then
    raise EkvFile.Create('Database list file: Invalid index');

  FFile.Position := KV_DatabaseListFile_HeaderSize +
                    Int64(Idx) * KV_DatabaseListFile_RecordSize;
end;

procedure TkvDatabaseListFile.LoadRecord(const Idx: Word32; var Rec: TkvDatabaseListFileRecord);
begin
  SeekRecord(Idx);
  FFile.ReadBuffer(Rec, KV_DatabaseListFile_RecordSize);

  if (Rec.Magic <> KV_DatabaseListFileRecord_Magic) or
     (Rec.Version = 0) then
    raise EkvFile.Create('Database list file: Corrupt record');
end;

procedure TkvDatabaseListFile.SaveRecord(const Idx: Word32; const Rec: TkvDatabaseListFileRecord);
begin
  SeekRecord(Idx);
  FFile.WriteBuffer(Rec, KV_DatabaseListFile_RecordSize);
end;

function TkvDatabaseListFile.AppendRecord(const Rec: TkvDatabaseListFileRecord): Word32;
var
  Idx : Word32;
begin
  Idx := FFileHeader.RecordCount;
  FFileHeader.RecordCount := Idx + 1;

  SeekRecord(Idx);
  FFile.WriteBuffer(Rec, KV_DatabaseListFile_RecordSize);

  SaveHeader;

  Result := Idx;
end;



{ TkvDatasetListFile }

constructor TkvDatasetListFile.Create(const Path, SystemName, DatabaseName: String);
begin
  if SystemName = '' then
    raise EkvFile.Create('System name required');
  if DatabaseName = '' then
    raise EkvFile.Create('Database name required');

  inherited Create;
  FPath := Path;
  FSystemName := SystemName;
  FDatabaseName := DatabaseName;

  PathEnsureSuffix(FPath);
  FFileName := FPath + FSystemName + '.' + FDatabaseName + '.kvdsl';
end;

procedure TkvDatasetListFile.OpenNew;
begin
  InitHeader;
  CreateFile;
  SaveHeader;
end;

procedure TkvDatasetListFile.Open;
begin
  OpenFile;
  LoadHeader;
end;

procedure TkvDatasetListFile.Close;
begin
  CloseFile;
end;

procedure TkvDatasetListFile.Delete;
begin
  DeleteFile(FFileName);
end;

procedure TkvDatasetListFile.CreateFile;
begin
  if FileExists(FFileName) then
    raise EkvFile.CreateFmt('Dataset list file exists: %s', [FFileName]);
  FFile := TFileStream.Create(FFileName, fmCreate or fmShareExclusive);
end;

procedure TkvDatasetListFile.OpenFile;
begin
  if not FileExists(FFileName) then
    raise EkvFile.CreateFmt('Dataset list file does not exists: %s', [FFileName]);
  FFile := TFileStream.Create(FFileName, fmOpenReadWrite or fmShareExclusive);
end;

procedure TkvDatasetListFile.CloseFile;
begin
  FreeAndNil(FFile);
end;

procedure TkvDatasetListFile.InitHeader;
begin
  kvInitDatasetListFileHeader(FFileHeader);
end;

procedure TkvDatasetListFile.LoadHeader;
var
  FileRecCount : Int64;
begin
  Assert(Assigned(FFile));

  if FFile.Size < KV_DatasetListFile_HeaderSize then
    raise EkvFile.Create('Dataset list file corrupt: File too small');

  FFile.Position := 0;
  FFile.ReadBuffer(FFileHeader, KV_DatasetListFile_HeaderSize);

  if (FFileHeader.Magic <> KV_DatasetListFileHeader_Magic) or
     (FFileHeader.Version = 0) or
     (FFileHeader.HeaderSize <> KV_DatasetListFile_HeaderSize) then
    raise EkvFile.Create('Dataset list file corrupt: Invalid header');

  FileRecCount := (FFile.Size - KV_DatasetListFile_HeaderSize) div
                  KV_DatasetListFile_RecordSize;
  if FileRecCount < FFileHeader.RecordCount then
    raise EkvFile.Create('Dataset list file corrupt: Missing records');
end;

procedure TkvDatasetListFile.SaveHeader;
begin
  Assert(Assigned(FFile));

  FFile.Position := 0;
  FFile.WriteBuffer(FFileHeader, KV_DatasetListFile_HeaderSize);
end;

function TkvDatasetListFile.GetHeader: PkvDatasetListFileHeader;
begin
  Result := @FFileHeader;
end;

procedure TkvDatasetListFile.HeaderModified;
begin
  SaveHeader;
end;

function TkvDatasetListFile.GetRecordCount: Integer;
begin
  Result := FFileHeader.RecordCount;
end;

procedure TkvDatasetListFile.SeekRecord(const Idx: Word32);
begin
  Assert(Assigned(FFile));

  if Idx >= FFileHeader.RecordCount then
    raise EkvFile.Create('Database list file: Invalid index');

  FFile.Position := KV_DatasetListFile_HeaderSize +
                    Int64(Idx) * KV_DatasetListFile_RecordSize;
end;

procedure TkvDatasetListFile.LoadRecord(const Idx: Word32; var Rec: TkvDatasetListFileRecord);
begin
  SeekRecord(Idx);
  FFile.ReadBuffer(Rec, KV_DatasetListFile_RecordSize);

  if (Rec.Magic <> KV_DatasetListFileRecord_Magic) or
     (Rec.Version = 0) then
    raise EkvFile.Create('Dataset list file: Corrupt record');
end;

procedure TkvDatasetListFile.SaveRecord(const Idx: Word32; const Rec: TkvDatasetListFileRecord);
begin
  SeekRecord(Idx);
  FFile.WriteBuffer(Rec, KV_DatasetListFile_RecordSize);
end;

function TkvDatasetListFile.AppendRecord(const Rec: TkvDatasetListFileRecord): Word32;
var
  Idx : Word32;
begin
  Idx := FFileHeader.RecordCount;
  FFileHeader.RecordCount := Idx + 1;

  SeekRecord(Idx);
  FFile.WriteBuffer(Rec, KV_DatasetListFile_RecordSize);

  SaveHeader;

  Result := Idx;
end;



{ TkvHashFile }

constructor TkvHashFile.Create(const Path, SystemName, DatabaseName, DatasetName: String;
            const CacheEntries: Word32);
var
  I : Integer;
begin
  if SystemName = '' then
    raise EkvFile.Create('System name required');
  if DatabaseName = '' then
    raise EkvFile.Create('Database name required');
  if DatasetName = '' then
    raise EkvFile.Create('Dataset name required');
  if CacheEntries > KV_HashFile_MaxCacheEntries then
    raise EkvFile.Create('Invalid cache entries value');

  inherited Create;

  FPath := Path;
  FSystemName := SystemName;
  FDatabaseName := DatabaseName;
  FDatasetName := DatasetName;

  FCacheEntries := CacheEntries;
  SetLength(FCacheRec, FCacheEntries);
  SetLength(FCacheValid, FCacheEntries);
  for I := 0 to FCacheEntries - 1 do
    FCacheValid[I] := False;

  PathEnsureSuffix(FPath);
  FFileName := FPath + FSystemName + '.' + FDatabaseName + '.' + FDatasetName + '.kvh';
end;

procedure TkvHashFile.OpenNew;
begin
  InitHeader;
  CreateFile;
  AllocateSlotRecords;
  SaveHeader;
  FHeaderModified := False;
end;

procedure TkvHashFile.Open;
begin
  OpenFile;
  LoadHeader;
  FHeaderModified := False;
end;

procedure TkvHashFile.Close;
begin
  CloseFile;
end;

procedure TkvHashFile.Delete;
begin
  DeleteFile(FFileName);
end;

procedure TkvHashFile.CreateFile;
begin
  if FileExists(FFileName) then
    raise EkvFile.CreateFmt('Hash file exists: %s', [FFileName]);
  FFile := TFileStream.Create(FFileName, fmCreate or fmShareExclusive);
end;

procedure TkvHashFile.OpenFile;
begin
  if not FileExists(FFileName) then
    raise EkvFile.CreateFmt('Hash file does not exists: %s', [FFileName]);
  FFile := TFileStream.Create(FFileName, fmOpenReadWrite or fmShareExclusive);
end;

procedure TkvHashFile.CloseFile;
begin
  FreeAndNil(FFile);
end;

procedure TkvHashFile.InitHeader;
begin
  kvInitHashFileHeader(FFileHeader);
end;

procedure TkvHashFile.LoadHeader;
var
  FileRecCount : Int64;
begin
  Assert(Assigned(FFile));

  if FFile.Size < KV_HashFile_HeaderSize then
    raise EkvFile.Create('Hash file corrupt: File too small');

  FFile.Position := 0;
  FFile.ReadBuffer(FFileHeader, KV_HashFile_HeaderSize);

  if (FFileHeader.Magic <> KV_HashFileHeader_Magic) or
     (FFileHeader.Version = 0) or
     (FFileHeader.HeaderSize <> KV_HashFile_HeaderSize) then
    raise EkvFile.Create('Hash file corrupt: Invalid header');

  FileRecCount := (FFile.Size - KV_HashFile_HeaderSize) div
                  KV_HashFile_RecordSize;
  if FileRecCount < FFileHeader.RecordCount then
    raise EkvFile.Create('Hash file corrupt: Missing records');
end;

procedure TkvHashFile.SaveHeader;
begin
  Assert(Assigned(FFile));

  FFile.Position := 0;
  FFile.WriteBuffer(FFileHeader, KV_HashFile_HeaderSize);
end;

function TkvHashFile.GetHeader: PkvHashFileHeader;
begin
  Result := @FFileHeader;
end;

procedure TkvHashFile.HeaderModified;
begin
  FHeaderModified := True;
end;

procedure TkvHashFile.UpdateHeader;
begin
  if FHeaderModified then
    begin
      SaveHeader;
      FHeaderModified := False;
    end;
end;

function TkvHashFile.AllocateUniqueId: UInt64;
var
  C : UInt64;
begin
  C := FFileHeader.UniqueIdCounter + 1;
  FFileHeader.UniqueIdCounter := C;
  HeaderModified;
  Result := C;
end;

function TkvHashFile.GetNextTimestamp: UInt64;
begin
  Result := FFileHeader.TimestampCounter + 1;
end;

procedure TkvHashFile.UpdateTimestamp(const Timestamp: UInt64);
begin
  FFileHeader.TimestampCounter := Timestamp;
  HeaderModified;
end;

function TkvHashFile.GetRecordCount: Integer;
begin
  Result := FFileHeader.RecordCount;
end;

procedure TkvHashFile.SeekRecord(const Idx: Word64);
begin
  Assert(Assigned(FFile));

  if Idx >= FFileHeader.RecordCount then
    raise EkvFile.Create('Hash file: Invalid index');

  FFile.Position := KV_HashFile_HeaderSize +
                    Idx * KV_HashFile_RecordSize;
end;

procedure TkvHashFile.LoadRecord(const Idx: Word64; var Rec: TkvHashFileRecord);
var
  CacheRange : Boolean;
begin
  CacheRange := Idx < Word32(FCacheEntries);
  if CacheRange then
    if FCacheValid[Idx] then
      begin
        Rec := FCacheRec[Idx];
        exit;
      end;

  SeekRecord(Idx);
  FFile.ReadBuffer(Rec, KV_HashFile_RecordSize);

  if (Rec.Magic <> KV_HashFileRecord_Magic) or
     (Rec.Version = 0) or
     not (Rec.RecordType in [hfrtEmpty, hfrtParentSlot, hfrtKeyValue,
         hfrtKeyValueWithHashCollision, hfrtDeleted]) or
     not (Rec.ValueType in [hfrvtNone, hfrvtShort, hfrvtLong, hfrvtFolder]) then
    raise EkvFile.Create('Hash file: Corrupt record');

  if CacheRange then
    begin
      FCacheRec[Idx] := Rec;
      FCacheValid[Idx] := True;
    end;
end;

procedure TkvHashFile.SaveRecord(const Idx: Word64; const Rec: TkvHashFileRecord);
begin
  Assert(Rec.Magic = KV_HashFileRecord_Magic);
  Assert(Rec.Version = KV_HashFileRecord_Version);
  Assert(Rec.RecordType in [hfrtEmpty, hfrtParentSlot, hfrtKeyValue,
      hfrtKeyValueWithHashCollision, hfrtDeleted]);
  Assert(Rec.ValueType in [hfrvtNone, hfrvtShort, hfrvtLong, hfrvtFolder]);

  SeekRecord(Idx);
  FFile.WriteBuffer(Rec, KV_HashFile_RecordSize);

  if Idx < Word32(FCacheEntries) then
    begin
      FCacheRec[Idx] := Rec;
      FCacheValid[Idx] := True;
    end;
end;

function TkvHashFile.AllocateSlotRecords: Word64;
var
  LvlSlots : Word32;
  RecIdx : Word64;
  RecCnt : Word64;
  I : Integer;
  EmpRec : TkvHashFileRecord;
  Rec : TkvHashFileRecord;
begin
  LvlSlots := FFileHeader.LevelSlotCount;

  Assert(LvlSlots = KV_HashFile_LevelSlotCount);
  Assert(Assigned(FFile));

  kvInitHashFileRecord(EmpRec);
  EmpRec.RecordType := hfrtEmpty;

  RecIdx := FFileHeader.FirstDeletedIndex;
  if RecIdx <> KV_HashFile_InvalidIndex then
    begin
      LoadRecord(RecIdx, Rec);
      for I := 0 to LvlSlots - 1 do
        SaveRecord(RecIdx + Word32(I), EmpRec);
      FFileHeader.FirstDeletedIndex := Rec.ChildSlotRecordIndex;
      HeaderModified;
      Result := RecIdx;
      exit;
    end;

  RecCnt := FFileHeader.RecordCount;
  FFileHeader.RecordCount := RecCnt + LvlSlots;
  try
    for I := 0 to LvlSlots - 1 do
      SaveRecord(RecCnt + Word32(I), EmpRec);
  except
    FFileHeader.RecordCount := RecCnt;
    raise;
  end;

  HeaderModified;
  Result := RecCnt;
end;

procedure TkvHashFile.AddDeletedSlots(const BaseIdx: Word64);
var
  HashRec : TkvHashFileRecord;
begin
  Assert(BaseIdx <> KV_HashFile_InvalidIndex);

  kvInitHashFileRecord(HashRec);
  HashRec.RecordType := hfrtDeleted;
  HashRec.ChildSlotRecordIndex := FFileHeader.FirstDeletedIndex;
  SaveRecord(BaseIdx, HashRec);

  FFileHeader.FirstDeletedIndex := BaseIdx;
  HeaderModified;
end;



{ TkvBlobFile }

constructor TkvBlobFile.Create(const Path, SystemName, DatabaseName, DatasetName, BlobName: String);
begin
  if SystemName = '' then
    raise EkvFile.Create('System name required');
  if DatabaseName = '' then
    raise EkvFile.Create('Database name required');
  if DatasetName = '' then
    raise EkvFile.Create('Dataset name required');
  if BlobName = '' then
    raise EkvFile.Create('Blob name required');

  inherited Create;
  FPath := Path;
  FSystemName := SystemName;
  FDatabaseName := DatabaseName;
  FDatasetName := DatasetName;
  FBlobName := BlobName;

  PathEnsureSuffix(FPath);
  FFileName := FPath + FSystemName + '.' + FDatabaseName + '.' + FDatasetName + '.' + FBlobName + '.kvbl';
end;

procedure TkvBlobFile.OpenNew(const RecordSize: Integer);
begin
  InitHeader(RecordSize);
  CreateFile;
  SaveHeader;
end;

procedure TkvBlobFile.Open;
begin
  OpenFile;
  LoadHeader;
end;

procedure TkvBlobFile.Close;
begin
  CloseFile;
end;

procedure TkvBlobFile.Delete;
begin
  DeleteFile(FFileName);
end;

procedure TkvBlobFile.CreateFile;
begin
  if FileExists(FFileName) then
    raise EkvFile.CreateFmt('Blob file exists: %s', [FFileName]);
  FFile := TFileStream.Create(FFileName, fmCreate or fmShareExclusive);
end;

procedure TkvBlobFile.OpenFile;
begin
  if not FileExists(FFileName) then
    raise EkvFile.CreateFmt('Blob file does not exists: %s', [FFileName]);
  FFile := TFileStream.Create(FFileName, fmOpenReadWrite or fmShareExclusive);
end;

procedure TkvBlobFile.CloseFile;
begin
  FreeAndNil(FFile);
end;

procedure TkvBlobFile.InitHeader(const RecordSize: Word32);
begin
  kvInitBlobFileHeader(FFileHeader, RecordSize);
end;

procedure TkvBlobFile.LoadHeader;
begin
  Assert(Assigned(FFile));

  if FFile.Size < KV_BlobFile_HeaderSize then
    raise EkvFile.Create('Blob file corrupt: File too small');

  FFile.Position := 0;
  FFile.ReadBuffer(FFileHeader, KV_BlobFile_HeaderSize);

  if (FFileHeader.Magic <> KV_BlobFileHeader_Magic) or
     (FFileHeader.Version = 0) or
     (FFileHeader.HeaderSize <> KV_BlobFile_HeaderSize) then
    raise EkvFile.Create('Blob file corrupt: Invalid header');

  if (FFileHeader.RecordSize < KV_BlobFile_MinRecordSize) or
     (FFileHeader.RecordSize mod KV_BlobFile_RecordSizeMultiple <> 0) then
    raise EkvFile.Create('Blob file corrupt: Invalid record size');
end;

procedure TkvBlobFile.SaveHeader;
begin
  Assert(Assigned(FFile));

  FFile.Position := 0;
  FFile.WriteBuffer(FFileHeader, KV_BlobFile_HeaderSize);
end;

procedure TkvBlobFile.SeekRecord(const Idx: Word64);
begin
  Assert(Assigned(FFile));
  Assert(FFileHeader.RecordSize > 0);

  FFile.Position := KV_BlobFile_HeaderSize +
                    Idx * FFileHeader.RecordSize;
end;

procedure TkvBlobFile.LoadRecordHeader(const Idx: Word64; var Hdr: TkvBlobFileRecordHeader);
begin
  SeekRecord(Idx);
  FFile.ReadBuffer(Hdr, KV_BlobFile_RecordHeaderSize);
  if (Hdr.Magic <> KV_BlobFileRecordHeader_Magic) or
     (Hdr.Version = 0) then
    raise EkvFile.Create('Blob file corrupt: Invalid record header');
end;

procedure TkvBlobFile.SaveRecordHeader(const Idx: Word64; const Hdr: TkvBlobFileRecordHeader);
begin
  SeekRecord(Idx);
  FFile.WriteBuffer(Hdr, KV_BlobFile_RecordHeaderSize);
end;

function TkvBlobFile.GetHeader: PkvBlobFileHeader;
begin
  Result := @FFileHeader;
end;

procedure TkvBlobFile.HeaderModified;
begin
  SaveHeader;
end;

function TkvBlobFile.GetRecordSize: Integer;
begin
  Result := FFileHeader.RecordSize;
end;

function TkvBlobFile.GetRecordDataSize: Integer;
begin
  if FFileHeader.RecordSize < KV_BlobFile_RecordHeaderSize then
    Result := 0
  else
    Result := FFileHeader.RecordSize - KV_BlobFile_RecordHeaderSize;
end;

function TkvBlobFile.GetRecordCount: Integer;
begin
  Result := FFileHeader.RecordCount;
end;

function TkvBlobFile.GetFreeRecordCount: Integer;
begin
  Result := FFileHeader.FreeRecordCount;
end;

function TkvBlobFile.AllocateRecord(var RecHdr: TkvBlobFileRecordHeader): Word64;
var
  RecIdx : Word64;
begin
  Assert(Assigned(FFile));

  RecIdx := FFileHeader.FreeRecordIndex;
  if RecIdx <> KV_BlobFile_InvalidIndex then
    begin
      LoadRecordHeader(RecIdx, RecHdr);
      FFileHeader.FreeRecordIndex := RecHdr.NextRecordIndex;
      Dec(FFileHeader.FreeRecordCount);
      RecHdr.NextRecordIndex := KV_BlobFile_InvalidIndex;
    end
  else
    begin
      RecIdx := FFileHeader.RecordCount;
      FFileHeader.RecordCount := RecIdx + 1;
      kvInitBlobFileRecordHeader(RecHdr);
    end;

  Result := RecIdx;
end;

// Returns a RecordIndex that can be used to access this chain again
function TkvBlobFile.CreateChain(const Buf; const BufSize: Integer): Word64;
var
  RecDataSize : Integer;
  FirstRecIdx : Word64;
  FirstRecHdr : TkvBlobFileRecordHeader;
  Remain : Integer;
  RecCnt : Integer;
  DataP : PByte;
  RecIdx : Word64;
  RecHdr : TkvBlobFileRecordHeader;
  DataSize : Integer;
  PrevRecHdr : TkvBlobFileRecordHeader;
  PrevRecIdx : Word64;
begin
  Assert(Assigned(FFile));
  if BufSize <= 0 then
    raise EkvFile.Create('Invalid buffer size');

  RecDataSize := GetRecordDataSize;
  FirstRecIdx := KV_BlobFile_InvalidIndex;
  PrevRecIdx := KV_BlobFile_InvalidIndex;
  Remain := BufSize;
  RecCnt := 0;
  DataP := @Buf;
  repeat
    Inc(RecCnt);

    RecIdx := AllocateRecord(RecHdr);

    SeekRecord(RecIdx);
    FFile.Write(RecHdr, KV_BlobFile_RecordHeaderSize);

    DataSize := Remain;
    if DataSize > RecDataSize then
      DataSize := RecDataSize;

    FFile.WriteBuffer(DataP^, DataSize);
    Dec(Remain, DataSize);
    Inc(DataP, DataSize);

    if RecCnt = 1 then
      begin
        FirstRecIdx := RecIdx;
        FirstRecHdr := RecHdr;
      end
    else
      begin
        Assert(PrevRecIdx <> KV_BlobFile_InvalidIndex);
        if RecCnt = 2 then
          FirstRecHdr.NextRecordIndex := RecIdx
        else
          begin
            PrevRecHdr.NextRecordIndex := RecIdx;
            SaveRecordHeader(PrevRecIdx, PrevRecHdr);
          end;
      end;

    if Remain > 0 then
      begin
        PrevRecHdr := RecHdr;
        PrevRecIdx := RecIdx;
      end;
  until Remain = 0;

  Assert(FirstRecIdx <> KV_BlobFile_InvalidIndex);
  FirstRecHdr.LastRecordIndex := RecIdx;
  FirstRecHdr.ChainSize := BufSize;
  SaveRecordHeader(FirstRecIdx, FirstRecHdr);

  SaveHeader;
  Result := FirstRecIdx;
end;

procedure TkvBlobFile.ReadChain(const RecordIndex: Word64; var Buf; const BufSize: Integer);
var
  RecDataSize : Integer;
  Remain : Integer;
  RecIdx : Word64;
  FirstRec : Boolean;
  RecHdr : TkvBlobFileRecordHeader;
  DataP : PByte;
  DataSize : Integer;
begin
  Assert(Assigned(FFile));
  Assert(RecordIndex <> KV_BlobFile_InvalidIndex);
  if BufSize <= 0 then
    raise EkvFile.Create('Invalid buffer size');

  RecDataSize := GetRecordDataSize;
  Remain := BufSize;
  DataP := @Buf;
  RecIdx := RecordIndex;
  FirstRec := True;
  repeat
    SeekRecord(RecIdx);
    FFile.ReadBuffer(RecHdr, KV_BlobFile_RecordHeaderSize);

    if FirstRec then
      begin
        if Word32(BufSize) > RecHdr.ChainSize then
          raise EkvFile.Create('Blob file read error: Chain size too short');
        FirstRec := False;
      end;

    DataSize := Remain;
    if DataSize > RecDataSize then
      DataSize := RecDataSize;

    FFile.ReadBuffer(DataP^, DataSize);
    Inc(DataP, DataSize);
    Dec(Remain, DataSize);

    if Remain > 0 then
      begin
        RecIdx := RecHdr.NextRecordIndex;
        if RecIdx = KV_BlobFile_InvalidIndex then
          raise EkvFile.Create('Blob file read error: Chain too short');
      end;
  until Remain = 0;
end;

procedure TkvBlobFile.ReleaseChain(const RecordIndex: Word64);
var
  RecIdx : Word64;
  RecHdr : TkvBlobFileRecordHeader;
  NextRecIdx : Word64;
begin
  Assert(RecordIndex <> KV_BlobFile_InvalidIndex);
  Assert(Assigned(FFile));

  RecIdx := RecordIndex;
  repeat
    LoadRecordHeader(RecIdx, RecHdr);
    NextRecIdx := RecHdr.NextRecordIndex;
    RecHdr.NextRecordIndex := FFileHeader.FreeRecordIndex;
    SaveRecordHeader(RecIdx, RecHdr);
    FFileHeader.FreeRecordIndex := RecIdx;
    Inc(FFileHeader.FreeRecordCount);
    RecIdx := NextRecIdx;
  until RecIdx = KV_BlobFile_InvalidIndex;

  SaveHeader;
end;

procedure TkvBlobFile.WriteChain(const RecordIndex: Word64; const Buf; const BufSize: Integer);
var
  RecDataSize : Integer;
  Remain : Integer;
  RecIdx : Word64;
  NewRecIdx : Word64;
  RecHdr : TkvBlobFileRecordHeader;
  FirstRecHdr : TkvBlobFileRecordHeader;
  FirstRecIdx : Word64;
  NewRecHdr : TkvBlobFileRecordHeader;
  DataP : PByte;
  DataSize : Integer;
  HdrChanged : Boolean;
  FreeRecIdx : Word64;
  NextFreeRecIdx : Word64;
  FreeRecHdr : TkvBlobFileRecordHeader;
begin
  Assert(Assigned(FFile));
  Assert(RecordIndex <> KV_BlobFile_InvalidIndex);
  if BufSize <= 0 then
    raise EkvFile.Create('Invalid buffer size');

  RecDataSize := GetRecordDataSize;
  Remain := BufSize;
  DataP := @Buf;
  RecIdx := RecordIndex;
  HdrChanged := False;

  Assert(RecIdx <> KV_BlobFile_InvalidIndex);
  SeekRecord(RecIdx);
  FFile.ReadBuffer(RecHdr, KV_BlobFile_RecordHeaderSize);
  FirstRecIdx := RecordIndex;
  FirstRecHdr := RecHdr;

  repeat
    DataSize := Remain;
    if DataSize > RecDataSize then
      DataSize := RecDataSize;

    FFile.WriteBuffer(DataP^, DataSize);
    Inc(DataP, DataSize);
    Dec(Remain, DataSize);

    if Remain > 0 then
      begin
        NewRecIdx := RecHdr.NextRecordIndex;
        if NewRecIdx = KV_BlobFile_InvalidIndex then
          begin
            NewRecIdx := AllocateRecord(NewRecHdr);
            if RecIdx = FirstRecIdx then
              FirstRecHdr.NextRecordIndex := NewRecIdx
            else
              begin
                RecHdr.NextRecordIndex := NewRecIdx;
                SaveRecordHeader(RecIdx, RecHdr);
              end;
            SaveRecordHeader(NewRecIdx, NewRecHdr);
            RecHdr := NewRecHdr;
            HdrChanged := True;
          end
        else
          begin
            SeekRecord(NewRecIdx);
            FFile.ReadBuffer(RecHdr, KV_BlobFile_RecordHeaderSize);
          end;
        RecIdx := NewRecIdx;
      end;
  until Remain = 0;

  FreeRecIdx := RecHdr.NextRecordIndex;
  if FreeRecIdx <> KV_BlobFile_InvalidIndex then
    begin
      if RecIdx = FirstRecIdx then
        FirstRecHdr.NextRecordIndex := KV_BlobFile_InvalidIndex
      else
        begin
          RecHdr.NextRecordIndex := KV_BlobFile_InvalidIndex;
          SaveRecordHeader(RecIdx, RecHdr);
        end;

      repeat
        LoadRecordHeader(FreeRecIdx, FreeRecHdr);
        NextFreeRecIdx := FreeRecHdr.NextRecordIndex;
        FreeRecHdr.NextRecordIndex := FFileHeader.FreeRecordIndex;
        SaveRecordHeader(FreeRecIdx, FreeRecHdr);
        FFileHeader.FreeRecordIndex := FreeRecIdx;
        Inc(FFileHeader.FreeRecordCount);
        FreeRecIdx := NextFreeRecIdx;
      until FreeRecIdx = KV_BlobFile_InvalidIndex;
      HdrChanged := True;
    end;

  FirstRecHdr.ChainSize := BufSize;
  FirstRecHdr.LastRecordIndex := RecIdx;
  SaveRecordHeader(FirstRecIdx, FirstRecHdr);

  if HdrChanged then
    SaveHeader;
end;

function TkvBlobFile.GetChainSize(const RecordIndex: Word64): Integer;
var
  RecHdr : TkvBlobFileRecordHeader;
begin
  Assert(Assigned(FFile));
  Assert(RecordIndex <> KV_BlobFile_InvalidIndex);

  LoadRecordHeader(RecordIndex, RecHdr);
  Result := RecHdr.ChainSize;
end;

procedure TkvBlobFile.AppendChain(const RecordIndex: Word64; const Buf; const BufSize: Integer);
var
  RecDataSize : Integer;
  Remain : Integer;
  RecIdx : Word64;
  NewRecIdx : Word64;
  RecHdr : TkvBlobFileRecordHeader;
  FirstRecHdr : TkvBlobFileRecordHeader;
  FirstRecIdx : Word64;
  LastBlockDataUsed : Integer;
  LastBlockDataRemain : Integer;
  NewRecHdr : TkvBlobFileRecordHeader;
  DataP : PByte;
  DataSize : Integer;
  HdrChanged : Boolean;

  procedure GetNewRecIdx;
  begin
    NewRecIdx := RecHdr.NextRecordIndex;
    if NewRecIdx = KV_BlobFile_InvalidIndex then
      begin
        NewRecIdx := AllocateRecord(NewRecHdr);
        RecHdr.NextRecordIndex := NewRecIdx;
        SaveRecordHeader(RecIdx, RecHdr);
        if RecIdx = FirstRecIdx then
          FirstRecHdr := RecHdr;
        FirstRecHdr.LastRecordIndex := NewRecIdx;
        SaveRecordHeader(NewRecIdx, NewRecHdr);
        RecHdr := NewRecHdr;
        HdrChanged := True;
      end
    else
      LoadRecordHeader(NewRecIdx, RecHdr);
    RecIdx := NewRecIdx;
  end;

begin
  Assert(Assigned(FFile));
  Assert(RecordIndex <> KV_BlobFile_InvalidIndex);
  if BufSize <= 0 then
    raise EkvFile.Create('Invalid buffer size');

  RecDataSize := GetRecordDataSize;
  Remain := BufSize;
  DataP := @Buf;
  HdrChanged := False;

  FirstRecIdx := RecordIndex;
  LoadRecordHeader(FirstRecIdx, FirstRecHdr);
  if Int64(FirstRecHdr.ChainSize) + BufSize > KV_BlobFile_MaxChainSize then
    raise EkvFile.Create('Blob file error: Chain size too large');

  RecIdx := FirstRecHdr.LastRecordIndex;
  if RecIdx = KV_BlobFile_InvalidIndex then
    raise EkvFile.Create('Blob file corrupt: Invalid record header');
  LoadRecordHeader(RecIdx, RecHdr);

  LastBlockDataUsed := Integer(FirstRecHdr.ChainSize) mod RecDataSize;
  if LastBlockDataUsed = 0 then
    LastBlockDataUsed := RecDataSize;
  LastBlockDataRemain := RecDataSize - LastBlockDataUsed;

  if LastBlockDataRemain > 0 then
    begin
      DataSize := Remain;
      if DataSize > LastBlockDataRemain then
        DataSize := LastBlockDataRemain;

      FFile.Position := KV_BlobFile_HeaderSize +
                        RecIdx * FFileHeader.RecordSize +
                        KV_BlobFile_RecordHeaderSize +
                        UInt64(LastBlockDataUsed);

      FFile.WriteBuffer(DataP^, DataSize);
      Inc(DataP, DataSize);
      Dec(Remain, DataSize);

      if Remain > 0 then
        GetNewRecIdx;
    end
  else
    GetNewRecIdx;

  while Remain > 0 do
    begin
      DataSize := Remain;
      if DataSize > RecDataSize then
        DataSize := RecDataSize;

      FFile.WriteBuffer(DataP^, DataSize);
      Inc(DataP, DataSize);
      Dec(Remain, DataSize);

      if Remain > 0 then
        GetNewRecIdx;
    end;

  FirstRecHdr.ChainSize := FirstRecHdr.ChainSize + Word32(BufSize);
  FirstRecHdr.LastRecordIndex := RecIdx;
  SaveRecordHeader(FirstRecIdx, FirstRecHdr);

  if HdrChanged then
    SaveHeader;
end;

procedure TkvBlobFile.WriteChainStart(const RecordIndex: Word64; const Buf; const BufSize: Integer);
var
  RecDataSize : Integer;
  RecHdr : TkvBlobFileRecordHeader;
begin
  Assert(Assigned(FFile));
  Assert(RecordIndex <> KV_BlobFile_InvalidIndex);
  if BufSize <= 0 then
    raise EkvFile.Create('Invalid buffer size');

  RecDataSize := GetRecordDataSize;
  if BufSize > RecDataSize then
    raise EkvFile.Create('Invalid buffer size: Larger than record data size');

  SeekRecord(RecordIndex);
  FFile.ReadBuffer(RecHdr, KV_BlobFile_RecordHeaderSize);
  if Word32(BufSize) > RecHdr.ChainSize then
    raise EkvFile.Create('Invalid buffer size: Larger than chain size');

  FFile.WriteBuffer(Buf, BufSize);
end;

procedure TkvBlobFile.TruncateChainAt(const RecordIndex: Word64; const Size: Integer); // Not used
var
  RecIdx : Word64;
  RecHdr : TkvBlobFileRecordHeader;
  FirstRecIdx : Word64;
  FirstRecHdr : TkvBlobFileRecordHeader;
  RecDataSize : Integer;
  Remain : Integer;
  HdrChanged : Boolean;
  FreeRecIdx : Word64;
  NextFreeRecIdx : Word64;
  FreeRecHdr : TkvBlobFileRecordHeader;
begin
  Assert(Assigned(FFile));
  Assert(RecordIndex <> KV_BlobFile_InvalidIndex);
  if Size < 0 then
    raise EkvFile.Create('Invalid chain size');

  RecIdx := RecordIndex;
  LoadRecordHeader(RecIdx, RecHdr);
  if Word32(Size) > RecHdr.ChainSize then
    raise EkvFile.Create('Invalid size: Larger than chain size');
  if Word32(Size) = RecHdr.ChainSize then
    exit;

  FirstRecIdx := RecIdx;
  FirstRecHdr := RecHdr;

  RecDataSize := GetRecordDataSize;
  Remain := Size;
  while Remain > RecDataSize do
    begin
      Dec(Remain, RecDataSize);
      RecIdx := RecHdr.NextRecordIndex;
      if RecIdx = KV_BlobFile_InvalidIndex then
        raise EkvFile.Create('Blob file corrupt: Invalid record header');
      LoadRecordHeader(RecIdx, RecHdr);
    end;

  HdrChanged := False;
  FreeRecIdx := RecHdr.NextRecordIndex;
  if FreeRecIdx <> KV_BlobFile_InvalidIndex then
    begin
      if RecIdx = FirstRecIdx then
        FirstRecHdr.NextRecordIndex := KV_BlobFile_InvalidIndex
      else
        begin
          RecHdr.NextRecordIndex := KV_BlobFile_InvalidIndex;
          SaveRecordHeader(RecIdx, RecHdr);
        end;

      repeat
        LoadRecordHeader(FreeRecIdx, FreeRecHdr);
        NextFreeRecIdx := FreeRecHdr.NextRecordIndex;
        FreeRecHdr.NextRecordIndex := FFileHeader.FreeRecordIndex;
        SaveRecordHeader(FreeRecIdx, FreeRecHdr);
        FFileHeader.FreeRecordIndex := FreeRecIdx;
        Inc(FFileHeader.FreeRecordCount);
        FreeRecIdx := NextFreeRecIdx;
      until FreeRecIdx = KV_BlobFile_InvalidIndex;
      HdrChanged := True;
    end;

  FirstRecHdr.LastRecordIndex := RecIdx;
  FirstRecHdr.ChainSize := Size;
  SaveRecordHeader(FirstRecIdx, FirstRecHdr);

  if HdrChanged then
    SaveHeader;
end;



end.

