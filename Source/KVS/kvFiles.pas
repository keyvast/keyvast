{ KeyVast - A key value store }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/02/07  0.01  Initial development }
{                   System file, Database list file }
{ 2018/02/08  0.02  Dataset list file }
{ 2018/02/09  0.03  Hash file }
{ 2018/02/10  0.04  Blob file }
{ 2018/02/18  0.05  Hash file cache }

// Todo: Pack files

{$INCLUDE kvInclude.inc}

unit kvFiles;

interface

uses
  {$IFDEF POSIX}
  Posix.Unistd,
  {$ENDIF}
  SysUtils,
  Classes,
  kvStructures;



type
  EkvFile = class(Exception);



{ AkvFile }

type
  AkvFile = class
  protected
    procedure LogWarning(const Txt: String);
  end;



{ TkvSystemFile }

type
  TkvSystemFile = class(AkvFile)
  private
    FPath       : String;
    FSystemName : String;

    FFileName   : String;
    FFileHeader : TkvSystemFileHeader;
    FFile       : TFileStream;

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
    destructor Destroy; override;

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
    FFile       : TFileStream;

    procedure CreateFile;
    procedure OpenFile;
    procedure CloseFile;
    procedure InitHeader;
    procedure LoadHeader;
    procedure SaveHeader;
    procedure SeekRecord(const Idx: Word32);

  public
    constructor Create(const Path, SystemName: String);
    destructor Destroy; override;

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
    FFile       : TFileStream;

    procedure CreateFile;
    procedure OpenFile;
    procedure CloseFile;
    procedure InitHeader;
    procedure LoadHeader;
    procedure SaveHeader;
    procedure SeekRecord(const Idx: Word32);

  public
    constructor Create(const Path, SystemName, DatabaseName: String);
    destructor Destroy; override;

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

type
  TkvHashFile = class(AkvFile)
  private
    FPath         : String;
    FSystemName   : String;
    FDatabaseName : String;
    FDatasetName  : String;

    FFileName     : String;
    FFileHeader   : TkvHashFileHeader;
    FFile         : TFileStream;
    FCacheEntries : Integer;
    FCacheRec     : array of TkvHashFileRecord;
    FCacheValid   : array of Boolean;

    procedure CreateFile;
    procedure OpenFile;
    procedure CloseFile;
    procedure InitHeader;
    procedure LoadHeader;
    procedure SaveHeader;
    procedure SeekRecord(const Idx: Word32);

  public
    constructor Create(const Path, SystemName, DatabaseName, DatasetName: String;
                const CacheEntries: Integer =
                    KV_HashFile_LevelSlotCount * KV_HashFile_LevelSlotCount);
    destructor Destroy; override;

    procedure OpenNew;
    procedure Open;
    procedure Close;
    procedure Delete;

    function  GetHeader: PkvHashFileHeader;
    procedure HeaderModified;
    function  AllocateUniqueId: UInt64;

    function  GetRecordCount: Integer;
    procedure LoadRecord(const Idx: Word32; var Rec: TkvHashFileRecord);
    procedure SaveRecord(const Idx: Word32; const Rec: TkvHashFileRecord);

    function  AllocateSlotRecords: Word32;

    procedure AddDeletedSlots(const BaseIdx: Word32);
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
    FFile       : TFileStream;

    procedure CreateFile;
    procedure OpenFile;
    procedure CloseFile;
    procedure InitHeader(const RecordSize: Word32);
    procedure LoadHeader;
    procedure SaveHeader;
    procedure SeekRecord(const Idx: Word32);
    procedure LoadRecordHeader(const Idx: Word32; var Hdr: TkvBlobFileRecordHeader);
    procedure SaveRecordHeader(const Idx: Word32; const Hdr: TkvBlobFileRecordHeader);
    function  AllocateRecord(var RecHdr: TkvBlobFileRecordHeader): Word32;

  public
    constructor Create(const Path, SystemName, DatabaseName, DatasetName, BlobName: String);
    destructor Destroy; override;

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

    function  CreateChain(const Buf; const BufSize: Integer): Word32;
    procedure ReadChain(const RecordIndex: Word32; var Buf; const BufSize: Integer);
    procedure ReleaseChain(const RecordIndex: Word32);
    procedure WriteChain(const RecordIndex: Word32; const Buf; const BufSize: Integer);
  end;



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

destructor TkvSystemFile.Destroy;
begin
  FreeAndNil(FFile);
  inherited Destroy;
end;

function TkvSystemFile.Exists: Boolean;
begin
  Result := FileExists(FFileName);
end;

procedure TkvSystemFile.OpenNew(const UserDataStr: String);
begin
  CreateFile;
  InitHeader(UserDataStr);
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

destructor TkvDatabaseListFile.Destroy;
begin
  inherited Destroy;
end;

procedure TkvDatabaseListFile.OpenNew;
begin
  CreateFile;
  InitHeader;
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

destructor TkvDatasetListFile.Destroy;
begin
  inherited Destroy;
end;

procedure TkvDatasetListFile.OpenNew;
begin
  CreateFile;
  InitHeader;
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
            const CacheEntries: Integer);
var
  I : Integer;
begin
  if SystemName = '' then
    raise EkvFile.Create('System name required');
  if DatabaseName = '' then
    raise EkvFile.Create('Database name required');
  if DatasetName = '' then
    raise EkvFile.Create('Dataset name required');
  if (CacheEntries < 0) or (CacheEntries > 262144) then
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

destructor TkvHashFile.Destroy;
begin
  inherited Destroy;
end;

procedure TkvHashFile.OpenNew;
begin
  CreateFile;
  InitHeader;
  AllocateSlotRecords;
  SaveHeader;
end;

procedure TkvHashFile.Open;
begin
  OpenFile;
  LoadHeader;
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
  SaveHeader;
end;

function TkvHashFile.AllocateUniqueId: UInt64;
var
  C : UInt64;
begin
  C := FFileHeader.UniqueIdCounter + 1;
  FFileHeader.UniqueIdCounter := C;
  SaveHeader;
  Result := C;
end;

function TkvHashFile.GetRecordCount: Integer;
begin
  Result := FFileHeader.RecordCount;
end;

procedure TkvHashFile.SeekRecord(const Idx: Word32);
begin
  Assert(Assigned(FFile));

  if Idx >= FFileHeader.RecordCount then
    raise EkvFile.Create('Hash file: Invalid index');

  FFile.Position := KV_HashFile_HeaderSize +
                    Int64(Idx) * KV_HashFile_RecordSize;
end;

procedure TkvHashFile.LoadRecord(const Idx: Word32; var Rec: TkvHashFileRecord);
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
     not (Rec.ValueType in [hfrvtNone, hfrvtShort, hfrvtLong]) then
    raise EkvFile.Create('Hash file: Corrupt record');

  if CacheRange then
    begin
      FCacheRec[Idx] := Rec;
      FCacheValid[Idx] := True;
    end;
end;

procedure TkvHashFile.SaveRecord(const Idx: Word32; const Rec: TkvHashFileRecord);
begin
  Assert(Rec.Magic = KV_HashFileRecord_Magic);
  Assert(Rec.Version = KV_HashFileRecord_Version);
  Assert(Rec.RecordType in [hfrtEmpty, hfrtParentSlot, hfrtKeyValue,
      hfrtKeyValueWithHashCollision, hfrtDeleted]);
  Assert(Rec.ValueType in [hfrvtNone, hfrvtShort, hfrvtLong]);

  SeekRecord(Idx);
  FFile.WriteBuffer(Rec, KV_HashFile_RecordSize);

  if Idx < Word32(FCacheEntries) then
    begin
      FCacheRec[Idx] := Rec;
      FCacheValid[Idx] := True;
    end;
end;

function TkvHashFile.AllocateSlotRecords: Word32;
var
  LvlSlots : Word32;
  RecIdx : Word32;
  RecCnt : Word32;
  I : Integer;
  EmpRec : TkvHashFileRecord;
  Rec : TkvHashFileRecord;
begin
  LvlSlots := FFileHeader.LevelSlotCount;

  Assert(LvlSlots = KV_HashFile_LevelSlotCount);
  Assert(Assigned(FFile));

  kvInitHashFileRecord(EmpRec);
  EmpRec.RecordType := hfrtEmpty;

  RecIdx := FFileHeader.FirstDeletedIdx;
  if RecIdx <> KV_HashFile_InvalidIndex then
    begin
      LoadRecord(RecIdx, Rec);
      FFileHeader.FirstDeletedIdx := Rec.ChildSlotRecordIndex;
      for I := 0 to LvlSlots - 1 do
        SaveRecord(RecIdx + Word32(I), EmpRec);
      SaveHeader;
      Result := RecIdx;
      exit;
    end;

  RecCnt := FFileHeader.RecordCount;
  FFileHeader.RecordCount := RecCnt + LvlSlots;
  for I := 0 to LvlSlots - 1 do
    SaveRecord(RecCnt + Word32(I), EmpRec);
  SaveHeader;
  Result := RecCnt;
end;

procedure TkvHashFile.AddDeletedSlots(const BaseIdx: Word32);
var
  HashRec : TkvHashFileRecord;
begin
  Assert(BaseIdx <> KV_HashFile_InvalidIndex);
  kvInitHashFileRecord(HashRec);
  HashRec.RecordType := hfrtDeleted;
  HashRec.ChildSlotRecordIndex := FFileHeader.FirstDeletedIdx;
  SaveRecord(BaseIdx, HashRec);
  FFileHeader.FirstDeletedIdx := BaseIdx;
  SaveHeader;
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

destructor TkvBlobFile.Destroy;
begin
  inherited Destroy;
end;

procedure TkvBlobFile.OpenNew(const RecordSize: Integer);
begin
  CreateFile;
  InitHeader(RecordSize);
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

  if (FFileHeader.RecordSize < KV_BlobFile_RecordHeaderSize) or
     (FFileHeader.RecordSize mod KV_BlobFile_RecordSizeMultiple <> 0) then
    raise EkvFile.Create('Blob file corrupt: Invalid record size');
end;

procedure TkvBlobFile.SaveHeader;
begin
  Assert(Assigned(FFile));

  FFile.Position := 0;
  FFile.WriteBuffer(FFileHeader, KV_BlobFile_HeaderSize);
end;

procedure TkvBlobFile.SeekRecord(const Idx: Word32);
begin
  Assert(Assigned(FFile));
  Assert(FFileHeader.RecordSize > 0);

  FFile.Position := KV_BlobFile_HeaderSize +
                    UInt64(Idx) * FFileHeader.RecordSize;
end;

procedure TkvBlobFile.LoadRecordHeader(const Idx: Word32; var Hdr: TkvBlobFileRecordHeader);
begin
  SeekRecord(Idx);
  FFile.ReadBuffer(Hdr, KV_BlobFile_RecordHeaderSize);
  if (Hdr.Magic <> KV_BlobFileRecordHeader_Magic) or
     (Hdr.Version = 0) then
    raise EkvFile.Create('Blob file corrupt: Invalid record header');
end;

procedure TkvBlobFile.SaveRecordHeader(const Idx: Word32; const Hdr: TkvBlobFileRecordHeader);
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

function TkvBlobFile.AllocateRecord(var RecHdr: TkvBlobFileRecordHeader): Word32;
var
  RecIdx : Word32;
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

function TkvBlobFile.CreateChain(const Buf; const BufSize: Integer): Word32;
var
  RecDataSize : Integer;
  FirstRecIdx : Word32;
  Remain : Integer;
  RecCnt : Integer;
  DataP : PByte;
  RecIdx : Word32;
  RecHdr : TkvBlobFileRecordHeader;
  DataSize : Integer;
  PrevRecHdr : TkvBlobFileRecordHeader;
  PrevRecIdx : Word32;
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
      FirstRecIdx := RecIdx
    else
      begin
        Assert(PrevRecIdx <> KV_BlobFile_InvalidIndex);
        PrevRecHdr.NextRecordIndex := RecIdx;
        SaveRecordHeader(PrevRecIdx, PrevRecHdr);
      end;

    if Remain > 0 then
      begin
        PrevRecHdr := RecHdr;
        PrevRecIdx := RecIdx;
      end;
  until Remain = 0;

  SaveHeader;
  Result := FirstRecIdx;
end;

procedure TkvBlobFile.ReadChain(const RecordIndex: Word32; var Buf; const BufSize: Integer);
var
  RecDataSize : Integer;
  Remain : Integer;
  RecIdx : Word32;
  RecHdr : TkvBlobFileRecordHeader;
  DataP : PByte;
  DataSize : Integer;
begin
  Assert(Assigned(FFile));
  if BufSize <= 0 then
    raise EkvFile.Create('Invalid buffer size');

  RecDataSize := GetRecordDataSize;
  Remain := BufSize;
  DataP := @Buf;
  RecIdx := RecordIndex;
  repeat
    SeekRecord(RecIdx);
    FFile.ReadBuffer(RecHdr, KV_BlobFile_RecordHeaderSize);

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

procedure TkvBlobFile.ReleaseChain(const RecordIndex: Word32);
var
  RecIdx : Word32;
  RecHdr : TkvBlobFileRecordHeader;
  NextRecIdx : Word32;
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

procedure TkvBlobFile.WriteChain(const RecordIndex: Word32; const Buf; const BufSize: Integer);
var
  RecDataSize : Integer;
  Remain : Integer;
  RecIdx : Word32;
  NewRecIdx : Word32;
  RecHdr : TkvBlobFileRecordHeader;
  NewRecHdr : TkvBlobFileRecordHeader;
  DataP : PByte;
  DataSize : Integer;
  HdrChanged : Boolean;
begin
  Assert(Assigned(FFile));
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
            RecHdr.NextRecordIndex := NewRecIdx;
            SaveRecordHeader(RecIdx, RecHdr);
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

  if HdrChanged then
    SaveHeader;
end;



end.

