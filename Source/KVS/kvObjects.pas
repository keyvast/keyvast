{ KeyVast - A key value store }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/02/07  0.01  Initial development }
{                   System, Database list, Database }
{ 2018/02/08  0.02  Dataset list, Dataset }
{ 2018/02/09  0.03  Dataset hash list }
{ 2018/02/10  0.04  Dataset blob files }
{ 2018/02/11  0.05  MultiSessionSystem, Session }
{ 2018/02/18  0.06  Handle hash collisions }
{ 2018/02/28  0.07  SysInfoDataset }
{ 2018/03/03  0.08  Folders support in Dataset }

{$INCLUDE kvInclude.inc}

unit kvObjects;

interface

uses
  SysUtils,
  kvHashList,
  kvStructures,
  kvFiles,
  kvValues;



type
  EkvObject = class(Exception);



  { TkvDataset }

  TkvDatasetIteratorStackEntry = record
    BaseRecIdx : Word32;
    SlotIdx    : Integer;
    FolderName : String;
  end;
  PkvDatasetIteratorStackEntry = ^TkvDatasetIteratorStackEntry;

  TkvDataset = class;

  TkvDatasetIterator = record
    DatabaseName : String;
    DatasetName  : String;
    Path         : String;
    Dataset      : TkvDataset;
    StackLen     : Integer;
    Stack        : array of TkvDatasetIteratorStackEntry;
    HashRecIdx   : Word32;
    HashRec      : TkvHashFileRecord;
  end;

  TkvDataset = class
  private
    FPath           : String;
    FSystemName     : String;
    FDatabaseName   : String;
    FDatasetListIdx : Word32;
    FDatasetListRec : TkvDatasetListFileRecord;
    FName           : String;

    FHashFile  : TkvHashFile;
    FKeyFile   : TkvBlobFile;
    FValueFile : TkvBlobFile;

    function  HashRecSameKey(const HashRec: TkvHashFileRecord;
              const Key: String; const KeyHash: UInt64;
              out HashCollision: Boolean): Boolean;
    procedure HashRecSetKey(var HashRec: TkvHashFileRecord;
              const Key: String; const KeyHash: UInt64);
    procedure HashRecReleaseValue(var HashRec: TkvHashFileRecord);
    procedure HashRecSetValue(var HashRec: TkvHashFileRecord;
              const Value: AkvValue);
    procedure HashRecInitKeyValue(out HashRec: TkvHashFileRecord;
              const Key: String; const KeyHash: UInt64;
              const Value: AkvValue;
              const IsFolder: Boolean; var FolderBaseIdx: Word32);
    procedure RecursiveHashRecSlotCollisionResolve(
              const HashBaseRecIdx, HashRecIdx: Word32; var HashRec: TkvHashFileRecord;
              const Key: String; const KeyHash: UInt64;
              const Value: AkvValue;
              const IsFolder: Boolean; var FolderBaseIdx: Word32;
              const CollisionLevel: Integer);
    procedure HashRecSlotCollisionResolve(
              const HashBaseRecIdx, HashRecIdx: Word32; var HashRec: TkvHashFileRecord;
              const Key: String; const KeyHash: UInt64;
              const Value: AkvValue;
              const IsFolder: Boolean; var FolderBaseIdx: Word32);
    procedure InternalAddRecord(const KeyBaseIdx: Word32; const Key: String;
              const Value: AkvValue;
              const IsFolder: Boolean; out FolderBaseIdx: Word32);
    function  LocateRecordFromBase(const BaseIndex: Word32; const Key: String;
              out HashRecIdx: Word32; out HashRec: TkvHashFileRecord): Boolean;
    function  LocateRecord(const Key: String;
              out HashRecIdx: Word32; out HashRec: TkvHashFileRecord): Boolean;
    function  HashRecToKey(const HashRec: TkvHashFileRecord): String;
    function  HashRecToValue(const HashRec: TkvHashFileRecord): AkvValue;
    procedure RecursiveGetChildRecords(const BaseIdx: Word32;
              const D: TkvDictionaryValue);
    function  RecursiveGetFolderRecords(const HashRec: TkvHashFileRecord): AkvValue;
    function  RecursiveGetAllRecords: AkvValue;
    procedure InternalDeleteRecord(const HashRecIdx: Word32; var HashRec: TkvHashFileRecord);
    function  SetNextIteratorRecord(var Iterator: TkvDatasetIterator): Boolean;

  public
    constructor Create(
                const Path, SystemName, DatabaseName: String;
                const DatasetListIdx: Word32;
                const DatasetListRec: TkvDatasetListFileRecord); overload;
    constructor Create(
                const Path, SystemName, DatabaseName, DatasetName: String); overload;
    destructor Destroy; override;

    property  Name: String read FName;

    procedure OpenNew;
    procedure Open;
    procedure Close;
    procedure Delete;

    function  AllocateUniqueId: UInt64;

    procedure AddRecord(const Key: String; const Value: AkvValue);
    procedure AddRecordString(const Key: String; const Value: String);
    procedure AddRecordInteger(const Key: String; const Value: Int64);
    procedure AddRecordFloat(const Key: String; const Value: Double);
    procedure AddRecordBoolean(const Key: String; const Value: Boolean);
    procedure AddRecordDateTime(const Key: String; const Value: TDateTime);
    procedure AddRecordNull(const Key: String);

    procedure MakePath(const Key: String);

    function  RecordExists(const Key: String): Boolean;

    function  GetRecord(const Key: String): AkvValue;
    function  GetRecordAsString(const Key: String): String;
    function  GetRecordAsInteger(const Key: String): Int64;
    function  GetRecordAsFloat(const Key: String): Double;
    function  GetRecordAsBoolean(const Key: String): Boolean;
    function  GetRecordAsDateTime(const Key: String): TDateTime;
    function  GetRecordIsNull(const Key: String): Boolean;

    procedure SetRecord(const Key: String; const Value: AkvValue);
    procedure SetRecordAsString(const Key: String; const Value: String);
    procedure SetRecordAsInteger(const Key: String; const Value: Int64);
    procedure SetRecordAsFloat(const Key: String; const Value: Double);
    procedure SetRecordAsBoolean(const Key: String; const Value: Boolean);
    procedure SetRecordAsDateTime(const Key: String; const Value: TDateTime);
    procedure SetRecordNull(const Key: String);

    procedure DeleteRecord(const Key: String);

    function  IterateRecords(const Path: String; out Iterator: TkvDatasetIterator): Boolean;
    function  IterateNextRecord(var Iterator: TkvDatasetIterator): Boolean;
    function  IteratorHasRecord(const Iterator: TkvDatasetIterator): Boolean;
    function  IteratorGetKey(const Iterator: TkvDatasetIterator): String;
    function  IteratorGetValue(const Iterator: TkvDatasetIterator): AkvValue;
  end;



  { TkvDatasetList }

  TkvDatasetList = class
  private
    FPath         : String;
    FSystemName   : String;
    FDatabaseName : String;

    FFile     : TkvDatasetListFile;
    FList     : array of TkvDataset;
    FHashList : TkvStringHashList;

    procedure ListClear;
    procedure ListAppend(const Item: TkvDataset);
    function  ListGetIndexByName(const Name: String): Integer;
    function  ListRemoveByIndex(const Index: Integer): TkvDataset;

  public
    constructor Create(const Path, SystemName, DatabaseName: String);
    destructor Destroy; override;

    procedure OpenNew;
    procedure Open;
    procedure Close;
    procedure Delete;

    function  GetCount: Integer;
    function  GetItem(const Idx: Integer): TkvDataset;

    function  Exists(const Name: String): Boolean;
    function  Add(const Name: String): TkvDataset;

    function  RequireItemByName(const Name: String): TkvDataset;

    procedure Remove(const Name: String);

    procedure SaveDataset(const Dataset: TkvDataset);
  end;



  { TkvDatabase }

  TkvDatabase = class
  private
    FPath            : String;
    FDatabaseListIdx : Word32;
    FDatabaseListRec : TkvDatabaseListFileRecord;

    FName        : String;
    FDatasetList : TkvDatasetList;

    function  AllocateUniqueId: UInt64;

  public
    constructor Create(
                const Path, SystemName: String;
                const DatabaseListIdx: Word32;
                const DatabaseListRec: TkvDatabaseListFileRecord);
    destructor Destroy; override;

    property  Name: String read FName;

    procedure OpenNew;
    procedure Open;
    procedure Close;
    procedure Delete;

    property  DatasetList: TkvDatasetList read FDatasetList;

    function  GetDatasetCount: Integer;
    function  GetDataset(const Idx: Integer): TkvDataset;

    function  RequireDatasetByName(const Name: String): TkvDataset;
    function  DatasetExists(const Name: String): Boolean;
    function  AddDataset(const Name: String): TkvDataset;
    procedure RemoveDataset(const Name: String);
  end;



  { TkvDatabaseList }

  TkvDatabaseList = class
  private
    FPath       : String;
    FSystemName : String;

    FFile     : TkvDatabaseListFile;
    FList     : array of TkvDatabase;
    FHashList : TkvStringHashList;

    procedure ListClear;
    procedure ListAppend(const Item: TkvDatabase);
    function  ListGetIndexByName(const Name: String): Integer;
    function  ListRemoveByIndex(const Index: Integer): TkvDatabase;

  public
    constructor Create(const Path, SystemName: String);
    destructor Destroy; override;

    procedure OpenNew;
    procedure Open;
    procedure Close;
    procedure Delete;

    function  GetCount: Integer;
    function  GetDatabase(const Idx: Integer): TkvDatabase;

    function  DatabaseExists(const Name: String): Boolean;
    function  AddDatabase(const Name: String): TkvDatabase;

    function  GetDatabaseByName(const Name: String): TkvDatabase;
    function  RequireDatabaseByName(const Name: String): TkvDatabase;

    procedure SaveDatabase(const Database: TkvDatabase);

    procedure Remove(const Name: String);
  end;



  { TkvSystem }

  TkvSystem = class
  private
    FPath : String;
    FName : String;

    FOpen           : Boolean;
    FSystemFile     : TkvSystemFile;
    FDatabaseList   : TkvDatabaseList;
    FSysInfoDataset : TkvDataset;

    procedure VerifyNotOpen;
    procedure VerifyOpen;

    function  GetUserDataStr: String;
    procedure SetUserDataStr(const A: String);

  public
    constructor Create(const Path, Name: String);
    destructor Destroy; override;

    function  Exists: Boolean;
    procedure OpenNew(const UserDataStr: String = '');
    procedure Open;
    procedure Close;
    procedure Delete;

    property  SysInfoDataset: TkvDataset read FSysInfoDataset;

    property  UserDataStr: String read GetUserDataStr write SetUserDataStr;
    function  AllocateSystemUniqueId: UInt64;

    function  GetDatabaseCount: Integer;
    function  GetDatabaseByIndex(const Idx: Integer): TkvDatabase;
    function  DatabaseExists(const Name: String): Boolean;
    function  CreateDatabase(const Name: String): TkvDatabase;
    function  RequireDatabaseByName(const Name: String): TkvDatabase;
    procedure DropDatabase(const Name: String);

    function  AllocateDatabaseUniqueId(const DatabaseName: String): UInt64;

    function  DatasetExists(const DatabaseName, DatasetName: String): Boolean;
    function  CreateDataset(const DatabaseName, DatasetName: String): TkvDataset;
    function  RequireDatasetByName(const DatabaseName, DatasetName: String): TkvDataset;
    procedure DropDataset(const DatabaseName, DatasetName: String);
    function  AllocateDatasetUniqueId(const DatabaseName, DatasetName: String): UInt64;

    procedure AddRecord(const DatabaseName, DatasetName, Key: String; const Value: AkvValue); overload;
    procedure AddRecord(const Dataset: TkvDataset; const Key: String; const Value: AkvValue); overload;

    procedure MakePath(const DatabaseName, DatasetName, KeyPath: String); overload;
    procedure MakePath(const Dataset: TkvDataset; const KeyPath: String); overload;

    function  RecordExists(const DatabaseName, DatasetName, Key: String): Boolean; overload;
    function  RecordExists(const Dataset: TkvDataset; const Key: String): Boolean; overload;

    function  GetRecord(const DatabaseName, DatasetName, Key: String): AkvValue; overload;
    function  GetRecord(const Dataset: TkvDataset; const Key: String): AkvValue; overload;

    procedure SetRecord(const DatabaseName, DatasetName, Key: String; const Value: AkvValue); overload;
    procedure SetRecord(const Dataset: TkvDataset; const Key: String; const Value: AkvValue); overload;

    procedure DeleteRecord(const DatabaseName, DatasetName, Key: String); overload;
    procedure DeleteRecord(const Dataset: TkvDataset; const Key: String); overload;

    function  IterateRecords(const DatabaseName, DatasetName: String;
              const Path: String;
              out Iterator: TkvDatasetIterator): Boolean;
    function  IterateNextRecord(var Iterator: TkvDatasetIterator): Boolean;
    function  IteratorGetKey(const Iterator: TkvDatasetIterator): String;
    function  IteratorGetValue(const Iterator: TkvDatasetIterator): AkvValue;
  end;



implementation

uses
  kvHash;



{ Helper functions }

procedure StringFromBuf(var S: String; const Buf; const Len: Integer);
begin
  SetLength(S, Len);
  if Len > 0 then
    Move(Buf, PChar(S)^, Len * SizeOf(Char));
end;



{ TkvDataset }

constructor TkvDataset.Create(
            const Path, SystemName, DatabaseName: String;
            const DatasetListIdx: Word32;
            const DatasetListRec: TkvDatasetListFileRecord);
begin
  Assert(SystemName <> '');
  Assert(DatabaseName <> '');

  inherited Create;

  FPath := Path;
  FSystemName := SystemName;
  FDatabaseName := DatabaseName;
  FDatasetListIdx := DatasetListIdx;
  FDatasetListRec := DatasetListRec;

  StringFromBuf(FName, FDatasetListRec.Name[0], FDatasetListRec.NameLength);

  FHashFile := TkvHashFile.Create(FPath, FSystemName, FDatabaseName, FName);
  FKeyFile := TkvBlobFile.Create(FPath, FSystemName, FDatabaseName, FName, 'k');
  FValueFile := TkvBlobFile.Create(FPath, FSystemName, FDatabaseName, FName, 'v');
end;

constructor TkvDataset.Create(const Path, SystemName, DatabaseName, DatasetName: String);
begin
  Assert(SystemName <> '');
  Assert(DatabaseName <> '');
  Assert(DatasetName <> '');

  inherited Create;

  FPath := Path;
  FSystemName := SystemName;
  FDatabaseName := DatabaseName;
  FName := DatasetName;

  FHashFile := TkvHashFile.Create(FPath, FSystemName, FDatabaseName, FName);
  FKeyFile := TkvBlobFile.Create(FPath, FSystemName, FDatabaseName, FName, 'k');
  FValueFile := TkvBlobFile.Create(FPath, FSystemName, FDatabaseName, FName, 'v');
end;

destructor TkvDataset.Destroy;
begin
  FreeAndNil(FValueFile);
  FreeAndNil(FKeyFile);
  FreeAndNil(FHashFile);
  inherited Destroy;
end;

procedure TkvDataset.OpenNew;
begin
  FHashFile.OpenNew;
  FKeyFile.OpenNew(128);
  FValueFile.OpenNew(1024);
end;

procedure TkvDataset.Open;
begin
  FHashFile.Open;
  FKeyFile.Open;
  FValueFile.Open;
end;

procedure TkvDataset.Close;
begin
  FValueFile.Close;
  FKeyFile.Close;
  FHashFile.Close;
end;

procedure TkvDataset.Delete;
begin
  FValueFile.Delete;
  FKeyFile.Delete;
  FHashFile.Delete;
end;

function TkvDataset.AllocateUniqueId: UInt64;
begin
  Result := FHashFile.AllocateUniqueId;
end;

// Returns True if HashRec's key matches Key
// HashCollision is set True if keys are different but have same hash
function TkvDataset.HashRecSameKey(const HashRec: TkvHashFileRecord;
         const Key: String; const KeyHash: UInt64;
         out HashCollision: Boolean): Boolean;
var
  KeyLen : Word32;
  R : Boolean;
  N, I : Integer;
begin
  HashCollision := False;
  if HashRec.KeyHash = KeyHash then
    begin
      KeyLen := Length(Key);
      Assert(KeyLen <= KV_HashFile_MaxKeyLength);
      if HashRec.KeyLength = KeyLen then
        begin
          R := True;
          N := KeyLen;
          if N > KV_HashFileRecord_SlotShortKeyLength then
            N := KV_HashFileRecord_SlotShortKeyLength;
          for I := 0 to N - 1 do
            if HashRec.KeyShort[I] <> Key[I + 1] then
              begin
                R := False;
                break;
              end;
          if R then
            if KeyLen > KV_HashFileRecord_SlotShortKeyLength then
              if HashRecToKey(HashRec) <> Key then
                R := False;
        end
      else
        R := False;
      if not R then
        HashCollision := True;
    end
  else
    R := False;
  Result := R;
end;

// Sets key in HashRec, allocating chain in keyfile if required
procedure TkvDataSet.HashRecSetKey(var HashRec: TkvHashFileRecord;
          const Key: String; const KeyHash: UInt64);
var
  KeyLen : Word32;
  N : Integer;
begin
  KeyLen := Length(Key);
  Assert(KeyLen <= KV_HashFile_MaxKeyLength);
  HashRec.KeyHash := KeyHash;
  HashRec.KeyLength := KeyLen;
  N := KeyLen;
  if N > KV_HashFileRecord_SlotShortKeyLength then
    N := KV_HashFileRecord_SlotShortKeyLength;
  Move(PChar(Key)^, HashRec.KeyShort[0], N * SizeOf(Char));
  if KeyLen > KV_HashFileRecord_SlotShortKeyLength then
    HashRec.KeyLongChainIndex := FKeyFile.CreateChain(PChar(Key)^, KeyLen * SizeOf(Char))
  else
    HashRec.KeyLongChainIndex := KV_BlobFile_InvalidIndex;
end;

procedure TkvDataSet.HashRecReleaseValue(var HashRec: TkvHashFileRecord);
begin
  if HashRec.ValueType = hfrvtLong then
    begin
      if HashRec.ValueLongChainIndex <> KV_BlobFile_InvalidIndex then
        begin
          FValueFile.ReleaseChain(HashRec.ValueLongChainIndex);
          HashRec.ValueLongChainIndex := KV_BlobFile_InvalidIndex;
        end;
      HashRec.ValueType := hfrvtNone;
    end;
end;

// Sets value in HashRec, allocating chain in valuefile if required
procedure TkvDataSet.HashRecSetValue(var HashRec: TkvHashFileRecord;
          const Value: AkvValue);
var
  M : Integer;
  ValBuf : Pointer;
begin
  M := Value.SerialSize;
  if M <= KV_HashFileRecord_SlotShortValueSize then
    begin
      HashRecReleaseValue(HashRec);
      HashRec.ValueType := hfrvtShort;
      Value.GetSerialBuf(HashRec.ValueShort[0], KV_HashFileRecord_SlotShortValueSize);
    end
  else
    begin
      GetMem(ValBuf, M);
      try
        Value.GetSerialBuf(ValBuf^, M);
        if HashRec.ValueType = hfrvtLong then
          begin
            Assert(HashRec.ValueLongChainIndex <> KV_BlobFile_InvalidIndex);
            FValueFile.WriteChain(HashRec.ValueLongChainIndex, ValBuf^, M)
          end
        else
          begin
            HashRec.ValueType := hfrvtLong;
            HashRec.ValueLongChainIndex := FValueFile.CreateChain(ValBuf^, M);
          end;
      finally
        FreeMem(ValBuf);
      end;
    end;
  HashRec.ValueSize := M;
  HashRec.ValueTypeId := Value.TypeId;
end;

procedure TkvDataset.HashRecInitKeyValue(out HashRec: TkvHashFileRecord;
          const Key: String; const KeyHash: UInt64;
          const Value: AkvValue;
          const IsFolder: Boolean; var FolderBaseIdx: Word32);
begin
  kvInitHashFileRecord(HashRec);
  HashRec.RecordType := hfrtKeyValue;
  if IsFolder then
    begin
      HashRec.ValueType := hfrvtFolder;
      HashRec.ValueFolderBaseIndex := FHashFile.AllocateSlotRecords;
      FolderBaseIdx := HashRec.ValueFolderBaseIndex;
    end
  else
    HashRecSetValue(HashRec, Value);
  HashRecSetKey(HashRec, Key, KeyHash);
end;

const
  KV_HashFile_MaxSlotCollisionCount = 6;

// Resolves slot collision between HashRec and Key
// HashBaseRecIdx and HashRecIdx is the base index and record index of HashRec
// When resolved new slot records with separate slot entries for HashRec and Key/Value is populated
procedure TkvDataset.RecursiveHashRecSlotCollisionResolve(
          const HashBaseRecIdx, HashRecIdx: Word32; var HashRec: TkvHashFileRecord;
          const Key: String; const KeyHash: UInt64;
          const Value: AkvValue;
          const IsFolder: Boolean; var FolderBaseIdx: Word32;
          const CollisionLevel: Integer);
var
  BaseIdx : Word32;
  RecIdx : Word32;
  Key1Hash : UInt64;
  Key2Hash : UInt64;
  Slt1 : Word32;
  Slt2 : Word32;
  ParentHashRec : TkvHashFileRecord;
  NewHashRec : TkvHashFileRecord;
begin
  if CollisionLevel > KV_HashFile_MaxSlotCollisionCount then
    raise EkvObject.Create('Hash failure: Too many slot collisions');

  Key1Hash := KeyHash;
  Key2Hash := HashRec.KeyHash;
  if Key1Hash = Key2Hash then
    raise EkvObject.Create('Hash failure: Hash collision');

  BaseIdx := FHashFile.AllocateSlotRecords;

  kvInitHashFileRecord(ParentHashRec);
  ParentHashRec.RecordType := hfrtParentSlot;
  ParentHashRec.ChildSlotRecordIndex := BaseIdx;

  Key1Hash := kvLevelNHash(Key1Hash);
  Key2Hash := kvLevelNHash(Key2Hash);

  HashRec.KeyHash := Key2Hash;

  Slt1 := Key1Hash mod KV_HashFile_LevelSlotCount;
  Slt2 := Key2Hash mod KV_HashFile_LevelSlotCount;
  if Slt1 = Slt2 then
    begin
      RecIdx := BaseIdx + Slt1;
      RecursiveHashRecSlotCollisionResolve(BaseIdx, RecIdx, HashRec,
          Key, Key1Hash, Value, IsFolder, FolderBaseIdx, CollisionLevel + 1);
    end
  else
    begin
      FHashFile.SaveRecord(BaseIdx + Slt2, HashRec);

      HashRecInitKeyValue(NewHashRec, Key, Key1Hash, Value, IsFolder, FolderBaseIdx);
      FHashFile.SaveRecord(BaseIdx + Slt1, NewHashRec);
    end;

  FHashFile.SaveRecord(HashRecIdx, ParentHashRec);
end;

procedure TkvDataset.HashRecSlotCollisionResolve(
          const HashBaseRecIdx, HashRecIdx: Word32; var HashRec: TkvHashFileRecord;
          const Key: String; const KeyHash: UInt64;
          const Value: AkvValue;
          const IsFolder: Boolean; var FolderBaseIdx: Word32);
begin
  Assert(HashRec.RecordType in [hfrtKeyValue, hfrtKeyValueWithHashCollision]);

  RecursiveHashRecSlotCollisionResolve(HashBaseRecIdx, HashRecIdx, HashRec,
      Key, KeyHash, Value, IsFolder, FolderBaseIdx, 1);

  FHashFile.HeaderModified;
end;

procedure TkvDataset.InternalAddRecord(const KeyBaseIdx: Word32; const Key: String;
          const Value: AkvValue;
          const IsFolder: Boolean; out FolderBaseIdx: Word32);
var
  Hsh : UInt64;
  SltI : Word32;
  HashRecBaseI : Word32;
  HashRecI : Word32;
  HashRec : TkvHashFileRecord;
  NewHashRec : TkvHashFileRecord;
  HashCol : Boolean;
  RecSl, RecI, EmpI : Word32;
  Fin : Boolean;
begin
  FolderBaseIdx := KV_HashFile_InvalidIndex;
  Hsh := kvLevel1HashString(Key, True);
  SltI := Hsh mod KV_HashFile_LevelSlotCount;
  HashRecBaseI := KeyBaseIdx;
  Fin := False;
  repeat
    HashRecI := HashRecBaseI + SltI;
    FHashFile.LoadRecord(HashRecI, HashRec);
    case HashRec.RecordType of
      hfrtEmpty :
        begin
          // Replace empty entry with key/value entry
          HashRecInitKeyValue(HashRec, Key, Hsh, Value, IsFolder, FolderBaseIdx);
          FHashFile.SaveRecord(HashRecI, HashRec);
          Fin := True;
        end;
      hfrtParentSlot :
        begin
          // Move on to child slots
          HashRecBaseI := HashRec.ChildSlotRecordIndex;
          Hsh := kvLevelNHash(Hsh);
          SltI := Hsh mod KV_HashFile_LevelSlotCount;
        end;
      hfrtKeyValue :
        begin
          if HashRecSameKey(HashRec, Key, Hsh, HashCol) then
            if IsFolder and (HashRec.ValueType = hfrvtFolder) then
              begin
                FolderBaseIdx := HashRec.ValueFolderBaseIndex;
                exit;
              end
            else
              raise EkvObject.CreateFmt('Key exists: %s', [Key]);
          if HashCol then
            begin
              // Change key/value entry to key/value-with-hash-collision entry
              HashRec.RecordType := hfrtKeyValueWithHashCollision;
              HashRec.ChildSlotRecordIndex := FHashFile.AllocateSlotRecords;
              FHashFile.SaveRecord(HashRecI, HashRec);
              // Save new key/value to child slot 0
              HashRecInitKeyValue(NewHashRec, Key, Hsh, Value, IsFolder, FolderBaseIdx);
              FHashFile.SaveRecord(HashRec.ChildSlotRecordIndex, NewHashRec);
            end
          else
            HashRecSlotCollisionResolve(HashRecBaseI, HashRecI, HashRec,
                Key, Hsh, Value, IsFolder, FolderBaseIdx);
          Fin := True;
        end;
      hfrtKeyValueWithHashCollision :
        begin
          // Check this key/value entry for duplicate key
          if HashRecSameKey(HashRec, Key, Hsh, HashCol) then
            raise EkvObject.CreateFmt('Key exists: %s', [Key]);
          if HashCol then
            begin
              // Check children for duplicate key and find first empty slot
              EmpI := KV_HashFile_InvalidIndex;
              for RecSl := 0 to KV_HashFile_LevelSlotCount - 1 do
                begin
                  RecI := HashRec.ChildSlotRecordIndex + RecSl;
                  FHashFile.LoadRecord(RecI, NewHashRec);
                  case NewHashRec.RecordType of
                    hfrtKeyValue :
                      if HashRecSameKey(NewHashRec, Key, Hsh, HashCol) then
                        raise EkvObject.CreateFmt('Key exists: %s', [Key]);
                    hfrtEmpty :
                      if EmpI = KV_HashFile_InvalidIndex then
                        EmpI := RecI;
                  end;
                end;
              if EmpI = KV_HashFile_InvalidIndex then // no more empty slots left in hash collision slots
                raise EkvObject.Create('Hash failure: Too many hash collisions');
              // Replace empty child slot with new key/value entry
              HashRecInitKeyValue(NewHashRec, Key, Hsh, Value, IsFolder, FolderBaseIdx);
              FHashFile.SaveRecord(EmpI, NewHashRec);
            end
          else
            // HashRec and its child collisions are moved as one
            HashRecSlotCollisionResolve(HashRecBaseI, HashRecI, HashRec,
                Key, Hsh, Value, IsFolder, FolderBaseIdx);
          Fin := True;
        end;
    else
      raise EkvObject.Create('Invalid hash record type');
    end;
  until Fin;
end;

procedure TkvDataset.AddRecord(const Key: String; const Value: AkvValue);
var
  KeyLen : Integer;
  BaseIdx : Word32;
  StartI : Integer;
  FolderSepI : Integer;
  SubKey : String;
begin
  KeyLen := Length(Key);
  if (KeyLen = 0) or (KeyLen > KV_HashFile_MaxKeyLength) then
    raise EkvObject.Create('Invalid key length');
  if Key.EndsWith('/') then
    raise EkvObject.Create('Invalid key: Folder reference');
  if not Assigned(Value) then
    raise EkvObject.Create('Invalid value');

  BaseIdx := 0;
  StartI := 0;
  repeat
    FolderSepI := Key.IndexOf(Char('/'), StartI);
    if FolderSepI = 0 then
      raise EkvObject.Create('Invalid key: Empty folder name');
    if FolderSepI > 0 then
      begin
        SubKey := Copy(Key, StartI + 1, FolderSepI - StartI);
        InternalAddRecord(BaseIdx, SubKey, nil, True, BaseIdx);
        StartI := FolderSepI + 1;
      end;
  until FolderSepI < 0;

  if StartI = 0 then
    SubKey := Key
  else
    SubKey := Copy(Key, StartI + 1, KeyLen - StartI);

  InternalAddRecord(BaseIdx, SubKey, Value, False, BaseIdx);
end;

procedure TkvDataset.AddRecordString(const Key: String; const Value: String);
var
  V : TkvStringValue;
begin
  V := TkvStringValue.Create(Value);
  try
    AddRecord(Key, V);
  finally
    V.Free;
  end;
end;

procedure TkvDataset.AddRecordInteger(const Key: String; const Value: Int64);
var
  V : TkvIntegerValue;
begin
  V := TkvIntegerValue.Create(Value);
  try
    AddRecord(Key, V);
  finally
    V.Free;
  end;
end;

procedure TkvDataset.AddRecordFloat(const Key: String; const Value: Double);
var
  V : TkvFloatValue;
begin
  V := TkvFloatValue.Create(Value);
  try
    AddRecord(Key, V);
  finally
    V.Free;
  end;
end;

procedure TkvDataset.AddRecordBoolean(const Key: String; const Value: Boolean);
var
  V : TkvBooleanValue;
begin
  V := TkvBooleanValue.Create(Value);
  try
    AddRecord(Key, V);
  finally
    V.Free;
  end;
end;

procedure TkvDataset.AddRecordDateTime(const Key: String; const Value: TDateTime);
var
  V : TkvDateTimeValue;
begin
  V := TkvDateTimeValue.Create(Value);
  try
    AddRecord(Key, V);
  finally
    V.Free;
  end;
end;

procedure TkvDataset.AddRecordNull(const Key: String);
var
  V : TkvNullValue;
begin
  V := TkvNullValue.Create;
  try
    AddRecord(Key, V);
  finally
    V.Free;
  end;
end;

procedure TkvDataset.MakePath(const Key: String);
var
  KeyLen : Integer;
  BaseIdx : Word32;
  StartI : Integer;
  FolderSepI : Integer;
  SubKey : String;
begin
  KeyLen := Length(Key);
  if (KeyLen = 0) or (KeyLen > KV_HashFile_MaxKeyLength) then
    raise EkvObject.Create('Invalid key length');

  BaseIdx := 0;
  StartI := 0;
  repeat
    FolderSepI := Key.IndexOf(Char('/'), StartI);
    if FolderSepI = 0 then
      raise EkvObject.Create('Invalid key: Empty folder name');
    if FolderSepI > 0 then
      begin
        SubKey := Copy(Key, StartI + 1, FolderSepI - StartI);
        InternalAddRecord(BaseIdx, SubKey, nil, True, BaseIdx);
        StartI := FolderSepI + 1;
      end;
  until FolderSepI < 0;
  if StartI = 0 then
    SubKey := Key
  else
    if StartI = KeyLen then
      exit
    else
      SubKey := Copy(Key, StartI + 1, KeyLen - StartI);
  InternalAddRecord(BaseIdx, SubKey, nil, True, BaseIdx);
end;

const
  KV_Dataset_LocateMaxLevels = 64;

function TkvDataset.LocateRecordFromBase(const BaseIndex: Word32; const Key: String;
         out HashRecIdx: Word32; out HashRec: TkvHashFileRecord): Boolean;
var
  Hsh : UInt64;
  SltI : Word32;
  HashRecBaseI : Word32;
  HashRecI : Word32;
  HashCol : Boolean;
  Level : Integer;
  R, Fin : Boolean;
begin
  Assert(Key <> '');

  Level := 0;
  R := False;
  Hsh := kvLevel1HashString(Key, True);
  SltI := Hsh mod KV_HashFile_LevelSlotCount;
  HashRecBaseI := BaseIndex;
  Fin := False;
  repeat
    Inc(Level);
    if Level = KV_Dataset_LocateMaxLevels then
      raise EkvObject.Create('Key too deep');
    HashRecI := HashRecBaseI + SltI;
    FHashFile.LoadRecord(HashRecI, HashRec);
    case HashRec.RecordType of
      hfrtEmpty :
        begin
          // No match
          R := False;
          Fin := True;
        end;
      hfrtParentSlot :
        begin
          // Move on to child slot
          HashRecBaseI := HashRec.ChildSlotRecordIndex;
          Hsh := kvLevelNHash(Hsh);
          SltI := Hsh mod KV_HashFile_LevelSlotCount;
        end;
      hfrtKeyValue :
        begin
          // Search complete, possible match, compare keys
          R := HashRecSameKey(HashRec, Key, Hsh, HashCol);
          if R then
            HashRecIdx := HashRecI;
          Fin := True;
        end;
      hfrtKeyValueWithHashCollision :
        begin
          // Check key/value entry first
          R := HashRecSameKey(HashRec, Key, Hsh, HashCol);
          if R then
            HashRecIdx := HashRecI
          else
            begin
              // Check collision slot entries for same key
              HashRecBaseI := HashRec.ChildSlotRecordIndex;
              for SltI := 0 to KV_HashFile_LevelSlotCount - 1 do
                begin
                  HashRecI := HashRecBaseI + SltI;
                  FHashFile.LoadRecord(HashRecI, HashRec);
                  R := HashRecSameKey(HashRec, Key, Hsh, HashCol);
                  if R then
                    begin
                      HashRecIdx := HashRecI;
                      break;
                    end;
                end;
            end;
          Fin := True;
        end;
    else
      raise EkvObject.Create('Invalid hash record type');
    end;
  until Fin;
  if not R then
    HashRecIdx := $FFFFFFFF;
  Result := R;
end;

function TkvDataset.LocateRecord(const Key: String;
         out HashRecIdx: Word32; out HashRec: TkvHashFileRecord): Boolean;
var
  KeyLen : Integer;
  BaseIdx : Word32;
  StartI : Integer;
  FolderSepI : Integer;
  SubKey : String;
begin
  KeyLen := Length(Key);
  BaseIdx := 0;
  StartI := 0;
  repeat
    FolderSepI := Key.IndexOf(Char('/'), StartI);
    if FolderSepI = 0 then
      raise EkvObject.Create('Invalid key: Empty folder name');
    if FolderSepI > 0 then
      begin
        SubKey := Copy(Key, StartI + 1, FolderSepI - StartI);
        Result := LocateRecordFromBase(BaseIdx, SubKey, HashRecIdx, HashRec);
        if not Result then
          exit;
        if HashRec.ValueType <> hfrvtFolder then
          raise EkvObject.Create('Invalid key: Not a folder');
        BaseIdx := HashRec.ValueFolderBaseIndex;
        StartI := FolderSepI + 1;
      end;
  until FolderSepI < 0;

  if StartI = 0 then
    SubKey := Key
  else
    SubKey := Copy(Key, StartI + 1, KeyLen - StartI);
  Result := LocateRecordFromBase(BaseIdx, SubKey, HashRecIdx, HashRec);
end;

function TkvDataset.RecordExists(const Key: String): Boolean;
var
  HashRecIdx : Word32;
  HashRec : TkvHashFileRecord;
begin
  if Key = '' then
    raise EkvObject.Create('Invalid key');
  Result := LocateRecord(Key, HashRecIdx, HashRec);
end;

function TkvDataset.HashRecToKey(const HashRec: TkvHashFileRecord): String;
var
  S : String;
begin
  Assert(HashRec.RecordType in [hfrtKeyValue, hfrtKeyValueWithHashCollision]);
  if HashRec.KeyLength <= KV_HashFileRecord_SlotShortKeyLength then
    begin
      Assert(HashRec.KeyLength > 0);
      SetLength(S, HashRec.KeyLength);
      Move(HashRec.KeyShort[0], PChar(S)^, HashRec.KeyLength * SizeOf(Char));
    end
  else
    begin
      SetLength(S, HashRec.KeyLength);
      FKeyFile.ReadChain(HashRec.KeyLongChainIndex, PChar(S)^, HashRec.KeyLength * SizeOf(Char));
    end;
  Result := S;
end;

function TkvDataset.HashRecToValue(const HashRec: TkvHashFileRecord): AkvValue;
var
  V : AkvValue;
  ValBuf : Pointer;
begin
  Assert(HashRec.RecordType in [hfrtKeyValue, hfrtKeyValueWithHashCollision]);

  case HashRec.ValueType of
    hfrvtNone  :
      raise EkvObject.Create('No value');
    hfrvtShort :
      begin
        V := kvCreateValueFromTypeId(HashRec.ValueTypeId);
        V.PutSerialBuf(HashRec.ValueShort[0], KV_HashFileRecord_SlotShortValueSize);
        Result := V;
      end;
    hfrvtLong  :
      begin
        Assert(HashRec.ValueLongChainIndex <> KV_BlobFile_InvalidIndex);
        Assert(HashRec.ValueSize > 0);
        V := kvCreateValueFromTypeId(HashRec.ValueTypeId);
        try
          GetMem(ValBuf, HashRec.ValueSize);
          try
            FValueFile.ReadChain(HashRec.ValueLongChainIndex, ValBuf^, HashRec.ValueSize);
            V.PutSerialBuf(ValBuf^, HashRec.ValueSize);
          finally
            FreeMem(ValBuf);
          end;
        except
          V.Free;
          raise;
        end;
        Result := V;
      end;
  else
    raise EkvObject.Create('Invalid value type');
  end;
end;

procedure TkvDataset.RecursiveGetChildRecords(const BaseIdx: Word32;
          const D: TkvDictionaryValue);
var
  I : Integer;
  RecI : Word32;
  ChildRec : TkvHashFileRecord;
  Key : String;
  Value : AkvValue;
begin
  for I := 0 to KV_HashFile_LevelSlotCount - 1 do
    begin
      RecI := BaseIdx + Word32(I);
      FHashFile.LoadRecord(RecI, ChildRec);
      case ChildRec.RecordType of
        hfrtEmpty : ;
        hfrtParentSlot :
          RecursiveGetChildRecords(ChildRec.ChildSlotRecordIndex, D);
        hfrtKeyValue :
          begin
            Key := HashRecToKey(ChildRec);
            if ChildRec.ValueType = hfrvtFolder then
              Value := RecursiveGetFolderRecords(ChildRec)
            else
              Value := HashRecToValue(ChildRec);
            D.Add(Key, Value);
          end;
        hfrtKeyValueWithHashCollision :
          begin
            Key := HashRecToKey(ChildRec);
            if ChildRec.ValueType = hfrvtFolder then
              Value := RecursiveGetFolderRecords(ChildRec)
            else
              Value := HashRecToValue(ChildRec);
            D.Add(Key, Value);
            RecursiveGetChildRecords(ChildRec.ChildSlotRecordIndex, D);
          end;
      end;
    end;
end;

function TkvDataset.RecursiveGetFolderRecords(const HashRec: TkvHashFileRecord): AkvValue;
var
  D : TkvDictionaryValue;
begin
  Assert(HashRec.ValueType = hfrvtFolder);
  D := TkvDictionaryValue.Create;
  try
    RecursiveGetChildRecords(HashRec.ValueFolderBaseIndex, D);
  except
    D.Free;
    raise;
  end;
  Result := D;
end;

function TkvDataset.RecursiveGetAllRecords: AkvValue;
var
  D : TkvDictionaryValue;
begin
  D := TkvDictionaryValue.Create;
  try
    RecursiveGetChildRecords(0, D);
  except
    D.Free;
    raise;
  end;
  Result := D;
end;

function TkvDataset.GetRecord(const Key: String): AkvValue;
var
  HashRecIdx : Word32;
  HashRec : TkvHashFileRecord;
begin
  if Key = '' then
    raise EkvObject.Create('Invalid key');
  if Key = '/' then
    Result := RecursiveGetAllRecords
  else
    begin
      if not LocateRecord(Key, HashRecIdx, HashRec) then
        raise EkvObject.CreateFmt('Key not found: %s', [Key]);
      if HashRec.ValueType = hfrvtFolder then
        Result := RecursiveGetFolderRecords(HashRec)
      else
        Result := HashRecToValue(HashRec);
    end;
end;

function TkvDataset.GetRecordAsString(const Key: String): String;
var
  V : AkvValue;
begin
  V := GetRecord(Key);
  try
    Result := V.AsString;
  finally
    V.Free;
  end;
end;

function TkvDataset.GetRecordAsInteger(const Key: String): Int64;
var
  V : AkvValue;
begin
  V := GetRecord(Key);
  try
    Result := V.AsInteger;
  finally
    V.Free;
  end;
end;

function TkvDataset.GetRecordAsFloat(const Key: String): Double;
var
  V : AkvValue;
begin
  V := GetRecord(Key);
  try
    Result := V.AsFloat;
  finally
    V.Free;
  end;
end;

function TkvDataset.GetRecordAsBoolean(const Key: String): Boolean;
var
  V : AkvValue;
begin
  V := GetRecord(Key);
  try
    Result := V.AsBoolean;
  finally
    V.Free;
  end;
end;

function TkvDataset.GetRecordAsDateTime(const Key: String): TDateTime;
var
  V : AkvValue;
begin
  V := GetRecord(Key);
  try
    Result := V.AsDateTime;
  finally
    V.Free;
  end;
end;

function TkvDataset.GetRecordIsNull(const Key: String): Boolean;
var
  V : AkvValue;
begin
  V := GetRecord(Key);
  try
    Result := V is TkvNullValue;
  finally
    V.Free;
  end;
end;

procedure TkvDataset.SetRecord(const Key: String; const Value: AkvValue);
var
  HashRecIdx : Word32;
  HashRec : TkvHashFileRecord;
begin
  if Key = '' then
    raise EkvObject.Create('Invalid key');
  if not LocateRecord(Key, HashRecIdx, HashRec) then
    raise EkvObject.CreateFmt('Key not found: %s', [Key]);
  if HashRec.ValueType = hfrvtFolder then
    raise EkvObject.CreateFmt('Key references a folder: %s', [Key]);
  HashRecSetValue(HashRec, Value);
  FHashFile.SaveRecord(HashRecIdx, HashRec);
end;

procedure TkvDataset.SetRecordAsString(const Key: String; const Value: String);
var
  V : TkvStringValue;
begin
  V := TkvStringValue.Create(Value);
  try
    SetRecord(Key, V);
  finally
    V.Free;
  end;
end;

procedure TkvDataset.SetRecordAsInteger(const Key: String; const Value: Int64);
var
  V : TkvIntegerValue;
begin
  V := TkvIntegerValue.Create(Value);
  try
    SetRecord(Key, V);
  finally
    V.Free;
  end;
end;

procedure TkvDataset.SetRecordAsFloat(const Key: String; const Value: Double);
var
  V : TkvFloatValue;
begin
  V := TkvFloatValue.Create(Value);
  try
    SetRecord(Key, V);
  finally
    V.Free;
  end;
end;

procedure TkvDataset.SetRecordAsBoolean(const Key: String; const Value: Boolean);
var
  V : TkvBooleanValue;
begin
  V := TkvBooleanValue.Create(Value);
  try
    SetRecord(Key, V);
  finally
    V.Free;
  end;
end;

procedure TkvDataset.SetRecordAsDateTime(const Key: String; const Value: TDateTime);
var
  V : TkvDateTimeValue;
begin
  V := TkvDateTimeValue.Create(Value);
  try
    SetRecord(Key, V);
  finally
    V.Free;
  end;
end;

procedure TkvDataset.SetRecordNull(const Key: String);
var
  V : TkvNullValue;
begin
  V := TkvNullValue.Create;
  try
    SetRecord(Key, V);
  finally
    V.Free;
  end;
end;

procedure TkvDataset.InternalDeleteRecord(const HashRecIdx: Word32; var HashRec: TkvHashFileRecord);
var
  SltI, RecI : Word32;
  ChildHashRec : TkvHashFileRecord;
  NewHashRec : TkvHashFileRecord;
  R : Boolean;
begin
  if HashRec.KeyLongChainIndex <> KV_BlobFile_InvalidIndex then
    begin
      FKeyFile.ReleaseChain(HashRec.KeyLongChainIndex);
      HashRec.KeyLongChainIndex := KV_BlobFile_InvalidIndex;
    end;
  if HashRec.ValueType = hfrvtFolder then
    begin
      for SltI := 0 to KV_HashFile_LevelSlotCount - 1 do
        begin
          RecI := HashRec.ValueFolderBaseIndex + SltI;
          FHashFile.LoadRecord(RecI, ChildHashRec);
          if ChildHashRec.RecordType in [hfrtKeyValue, hfrtKeyValueWithHashCollision] then
            InternalDeleteRecord(RecI, ChildHashRec);
        end;
      FHashFile.AddDeletedSlots(HashRec.ValueFolderBaseIndex);
      HashRec.ValueFolderBaseIndex := KV_HashFile_InvalidIndex;
    end;
  HashRecReleaseValue(HashRec);
  case HashRec.RecordType of
    hfrtKeyValue :
      HashRec.RecordType := hfrtEmpty;
    hfrtKeyValueWithHashCollision :
      begin
        // Replace entry with first non-empty child entry and
        // set that child entry empty
        R := False;
        for SltI := 0 to KV_HashFile_LevelSlotCount - 1 do
          begin
            RecI := HashRec.ChildSlotRecordIndex + SltI;
            FHashFile.LoadRecord(RecI, ChildHashRec);
            if ChildHashRec.RecordType = hfrtKeyValue then
              begin
                NewHashRec := ChildHashRec;
                NewHashRec.RecordType := hfrtKeyValueWithHashCollision;
                NewHashRec.ChildSlotRecordIndex := HashRec.ChildSlotRecordIndex;
                HashRec := NewHashRec;

                ChildHashRec.RecordType := hfrtEmpty;
                FHashFile.SaveRecord(RecI, ChildHashRec);
                R := True;
                break;
              end;
          end;
        if not R then // All slots empty
          begin
            FHashFile.AddDeletedSlots(HashRec.ChildSlotRecordIndex);
            HashRec.ChildSlotRecordIndex := KV_HashFile_InvalidIndex;
            HashRec.RecordType := hfrtEmpty;
          end;
      end;
  end;
  FHashFile.SaveRecord(HashRecIdx, HashRec);
end;

procedure TkvDataset.DeleteRecord(const Key: String);
var
  HashRecIdx : Word32;
  HashRec : TkvHashFileRecord;
begin
  if Key = '' then
    raise EkvObject.Create('Invalid key');
  if not LocateRecord(Key, HashRecIdx, HashRec) then
    raise EkvObject.CreateFmt('Key not found: %s', [Key]);
  Assert(HashRec.RecordType in [hfrtKeyValue, hfrtKeyValueWithHashCollision]);
  InternalDeleteRecord(HashRecIdx, HashRec);
end;

function TkvDataset.SetNextIteratorRecord(var Iterator: TkvDatasetIterator): Boolean;
var
  E : PkvDatasetIteratorStackEntry;
  R : Boolean;
  RecIdx : Word32;
begin
  Assert(Iterator.StackLen > 0);

  E := @Iterator.Stack[Iterator.StackLen - 1];
  repeat

    R := False;
    repeat
      if E^.SlotIdx = KV_HashFile_LevelSlotCount then
        begin
          Dec(Iterator.StackLen);
          SetLength(Iterator.Stack, Iterator.StackLen);
          if Iterator.StackLen = 0 then
            begin
              Result := False;
              exit;
            end;
          E := @Iterator.Stack[Iterator.StackLen - 1];
        end
      else
        R := True;
    until R;

    R := False;
    repeat
      Assert(Word32(E^.SlotIdx) < KV_HashFile_LevelSlotCount);
      RecIdx := E^.BaseRecIdx + Word32(E^.SlotIdx);
      FHashFile.LoadRecord(RecIdx, Iterator.HashRec);
      case Iterator.HashRec.RecordType of
        hfrtKeyValue :
          if Iterator.HashRec.ValueType = hfrvtFolder then
            begin
              // Recurse down into folder slots
              Inc(E^.SlotIdx);
              Inc(Iterator.StackLen);
              SetLength(Iterator.Stack, Iterator.StackLen);
              E := @Iterator.Stack[Iterator.StackLen - 1];
              E^.BaseRecIdx := Iterator.HashRec.ValueFolderBaseIndex;
              E^.SlotIdx := 0;
              E^.FolderName := HashRecToKey(Iterator.HashRec);
            end
          else
            begin
              // Found key/value entry
              Result := True;
              exit;
            end;
        hfrtEmpty :
          begin
            // Empty slot, move on to next slot
            Inc(E^.SlotIdx);
            if E^.SlotIdx = KV_HashFile_LevelSlotCount then
              R := True;
          end;
        hfrtParentSlot :
          begin
            // Recurse down into child slots
            Inc(E^.SlotIdx);
            Inc(Iterator.StackLen);
            SetLength(Iterator.Stack, Iterator.StackLen);
            E := @Iterator.Stack[Iterator.StackLen - 1];
            E^.BaseRecIdx := Iterator.HashRec.ChildSlotRecordIndex;
            E^.SlotIdx := 0;
          end;
        hfrtKeyValueWithHashCollision :
          begin
            // Recurse down and use current key/value entry
            Inc(E^.SlotIdx);
            Inc(Iterator.StackLen);
            SetLength(Iterator.Stack, Iterator.StackLen);
            E := @Iterator.Stack[Iterator.StackLen - 1];
            E^.BaseRecIdx := Iterator.HashRec.ChildSlotRecordIndex;
            E^.SlotIdx := -1;
            Result := True;
            exit;
          end;
      else
        raise EkvObject.Create('Bad hash record type');
      end;
    until R;
  until false;
end;

function TkvDataset.IterateRecords(const Path: String; out Iterator: TkvDatasetIterator): Boolean;
var
  E : PkvDatasetIteratorStackEntry;
  BaseRecIdx : Word32;
  HashRecIdx : Word32;
  HashRec : TkvHashFileRecord;
begin
  if Path = '' then
    BaseRecIdx := 0
  else
    begin
      if not LocateRecord(Path, HashRecIdx, HashRec) then
        raise EkvObject.CreateFmt('Path not found: %s', [Path]);
      if HashRec.ValueType <> hfrvtFolder then
        raise EkvObject.CreateFmt('Path not a folder: %s', [Path]);
      BaseRecIdx := HashRec.ValueFolderBaseIndex;
    end;
  Iterator.Path := Path;
  Iterator.Dataset := self;
  Iterator.StackLen := 1;
  SetLength(Iterator.Stack, 1);
  E := @Iterator.Stack[0];
  E^.BaseRecIdx := BaseRecIdx;
  E^.SlotIdx := 0;
  Result := SetNextIteratorRecord(Iterator);
end;

function TkvDataset.IterateNextRecord(var Iterator: TkvDatasetIterator): Boolean;
var
  StackLen : Integer;
  E : PkvDatasetIteratorStackEntry;
begin
  Assert(Iterator.Dataset = self);

  StackLen := Iterator.StackLen;
  if StackLen = 0 then
    raise EkvObject.Create('Next past end');
  E := @Iterator.Stack[StackLen - 1];
  Inc(E^.SlotIdx);

  Result := SetNextIteratorRecord(Iterator);
end;

function TkvDataset.IteratorHasRecord(const Iterator: TkvDatasetIterator): Boolean;
begin
  Assert(Iterator.Dataset = self);
  Result := Iterator.StackLen > 0;
end;

function TkvDataset.IteratorGetKey(const Iterator: TkvDatasetIterator): String;
var
  S, F : String;
  I : Integer;
begin
  Assert(Iterator.Dataset = self);
  S := HashRecToKey(Iterator.HashRec);
  for I := Iterator.StackLen - 1 downto 0 do
    begin
      F := Iterator.Stack[I].FolderName;
      if F <> '' then
        S := F + '/' + S;
    end;
  F := Iterator.Path;
  if F <> '' then
    S := F + '/' + S;
  Result := S;
end;

function TkvDataset.IteratorGetValue(const Iterator: TkvDatasetIterator): AkvValue;
begin
  Assert(Iterator.Dataset = self);
  Result := HashRecToValue(Iterator.HashRec);
end;



{ TkvDatasetList }

constructor TkvDatasetList.Create(const Path, SystemName, DatabaseName: String);
begin
  Assert(SystemName <> '');
  Assert(DatabaseName <> '');

  inherited Create;
  FPath := Path;
  FSystemName := SystemName;
  FDatabaseName := DatabaseName;

  FHashList := TkvStringHashList.Create(False, False, False);
  FFile := TkvDatasetListFile.Create(Path, SystemName, DatabaseName);
end;

destructor TkvDatasetList.Destroy;
var
  I : Integer;
begin
  FreeAndNil(FFile);
  FreeAndNil(FHashList);
  for I := Length(FList) - 1 downto 0 do
    FreeAndNil(FList[I]);
  inherited Destroy;
end;

procedure TkvDatasetList.ListClear;
var
  I : Integer;
begin
  for I := Length(FList) - 1 downto 0 do
    FreeAndNil(FList[I]);
  SetLength(FList, 0);
  FHashList.Clear;
end;

procedure TkvDatasetList.ListAppend(const Item: TkvDataset);
var
  L : Integer;
begin
  Assert(Assigned(Item));

  L := Length(FList);
  SetLength(FList, L + 1);
  FList[L] := Item;

  FHashList.Add(Item.Name, Item);
end;

function TkvDatasetList.ListGetIndexByName(const Name: String): Integer;
var
  I : Integer;
begin
  for I := 0 to Length(FList) - 1 do
    if SameText(FList[I].Name, Name) then
      begin
        Result := I;
        exit;
      end;
  Result := -1;
end;

function TkvDatasetList.ListRemoveByIndex(const Index: Integer): TkvDataset;
var
  I, L : Integer;
begin
  L := Length(FList);
  Result := FList[Index];
  for I := Index to L - 2 do
    FList[I] := FList[I + 1];
  SetLength(FList, L - 1);
end;

procedure TkvDatasetList.OpenNew;
begin
  FFile.OpenNew;
end;

procedure TkvDatasetList.Open;
var
  RecIdx : Integer;
  Rec : TkvDatasetListFileRecord;
  Dataset : TkvDataset;
begin
  ListClear;
  FFile.Open;
  for RecIdx := 0 to FFile.GetRecordCount - 1 do
    begin
      FFile.LoadRecord(RecIdx, Rec);
      if not (dslfrfDeleted in Rec.Flags) then
        begin
          Dataset := TkvDataset.Create(FPath, FSystemName, FDatabaseName, RecIdx, Rec);
          ListAppend(Dataset);
          Dataset.Open;
        end;
    end;
end;

procedure TkvDatasetList.Close;
var
  I : Integer;
begin
  for I := 0 to Length(FList) - 1 do
    FList[I].Close;
  FFile.Close;
end;

procedure TkvDatasetList.Delete;
var
  I : Integer;
begin
  for I := 0 to Length(FList) - 1 do
    FList[I].Delete;
  FFile.Delete;
end;

function TkvDatasetList.GetCount: Integer;
begin
  Result := Length(FList);
end;

function TkvDatasetList.GetItem(const Idx: Integer): TkvDataset;
begin
  Assert(Idx >= 0);
  Assert(Idx < Length(FList));

  Result := FList[Idx];
end;

function TkvDatasetList.Exists(const Name: String): Boolean;
begin
  Result := FHashList.KeyExists(Name);
end;

function TkvDatasetList.Add(const Name: String): TkvDataset;
var
  Rec : TkvDatasetListFileRecord;
  RecIdx : Word32;
  Dataset : TkvDataset;
begin
  kvInitDatasetListFileRecord(Rec, Name);
  RecIdx := FFile.AppendRecord(Rec);
  Dataset := TkvDataset.Create(FPath, FSystemName, FDatabaseName, RecIdx, Rec);
  ListAppend(Dataset);
  Result := Dataset;
end;

function TkvDatasetList.RequireItemByName(const Name: String): TkvDataset;
begin
  Result := TkvDataset(FHashList.RequireValue(Name));
end;

procedure TkvDatasetList.Remove(const Name: String);
var
  I : Integer;
  Ds : TkvDataset;
begin
  Assert(Assigned(FFile));

  I := ListGetIndexByName(Name);
  if I < 0 then
    raise EkvObject.CreateFmt('Dataset not found: %s', [Name]);
  FHashList.DeleteKey(Name);
  Ds := ListRemoveByIndex(I);
  Include(Ds.FDatasetListRec.Flags, dslfrfDeleted);
  FFile.SaveRecord(Ds.FDatasetListIdx, Ds.FDatasetListRec);
  Ds.Free;
end;

procedure TkvDatasetList.SaveDataset(const Dataset: TkvDataset);
begin
  Assert(Assigned(Dataset));
  Assert(Assigned(FFile));

  FFile.SaveRecord(Dataset.FDatasetListIdx, Dataset.FDatasetListRec);
end;



{ TkvDatabase }

constructor TkvDatabase.Create(
            const Path, SystemName: String;
            const DatabaseListIdx: Word32;
            const DatabaseListRec: TkvDatabaseListFileRecord);
begin
  Assert(SystemName <> '');

  inherited Create;

  FPath := Path;
  FDatabaseListIdx := DatabaseListIdx;
  FDatabaseListRec := DatabaseListRec;

  StringFromBuf(FName, FDatabaseListRec.Name[0], FDatabaseListRec.NameLength);

  FDatasetList := TkvDatasetList.Create(Path, SystemName, FName);
end;

destructor TkvDatabase.Destroy;
begin
  FreeAndNil(FDatasetList);
  inherited Destroy;
end;

procedure TkvDatabase.OpenNew;
begin
  FDatasetList.OpenNew;
end;

procedure TkvDatabase.Open;
begin
  FDatasetList.Open;
end;

procedure TkvDatabase.Close;
begin
  FDatasetList.Close;
end;

procedure TkvDatabase.Delete;
begin
  FDatasetList.Delete;
end;

function TkvDatabase.AllocateUniqueId: UInt64;
var
  C : UInt64;
begin
  C := FDatabaseListRec.UniqueIdCounter + 1;
  FDatabaseListRec.UniqueIdCounter := C;
  Result := C;
end;

function TkvDatabase.GetDatasetCount: Integer;
begin
  Result := FDatasetList.GetCount;
end;

function TkvDatabase.GetDataset(const Idx: Integer): TkvDataset;
begin
  Result := FDatasetList.GetItem(Idx);
end;

function TkvDatabase.RequireDatasetByName(const Name: String): TkvDataset;
begin
  Result := FDatasetList.RequireItemByName(Name);
end;

function TkvDatabase.DatasetExists(const Name: String): Boolean;
begin
  Result := FDatasetList.Exists(Name);
end;

function TkvDatabase.AddDataset(const Name: String): TkvDataset;
var
  Dataset : TkvDataSet;
begin
  Dataset := FDatasetList.Add(Name);
  Result := Dataset;
end;

procedure TkvDatabase.RemoveDataset(const Name: String);
begin
  FDatasetList.Remove(Name);
end;



{ TkvDatabaseList }

constructor TkvDatabaseList.Create(const Path, SystemName: String);
begin
  Assert(SystemName <> '');

  inherited Create;
  FPath := Path;
  FSystemName := SystemName;

  FHashList := TkvStringHashList.Create(False, False, False);
  FFile := TkvDatabaseListFile.Create(Path, SystemName);
end;

destructor TkvDatabaseList.Destroy;
var
  I : Integer;
begin
  FreeAndNil(FFile);
  FreeAndNil(FHashList);
  for I := Length(FList) - 1 downto 0 do
    FreeAndNil(FList[I]);
  inherited Destroy;
end;

procedure TkvDatabaseList.ListClear;
var
  I : Integer;
begin
  for I := Length(FList) - 1 downto 0 do
    FreeAndNil(FList[I]);
  SetLength(FList, 0);
  FHashList.Clear;
end;

procedure TkvDatabaseList.ListAppend(const Item: TkvDatabase);
var
  L : Integer;
begin
  Assert(Assigned(Item));

  L := Length(FList);
  SetLength(FList, L + 1);
  FList[L] := Item;

  FHashList.Add(Item.Name, Item);
end;

function TkvDatabaseList.ListGetIndexByName(const Name: String): Integer;
var
  I : Integer;
begin
  for I := 0 to Length(FList) - 1 do
    if SameText(FList[I].Name, Name) then
      begin
        Result := I;
        exit;
      end;
  Result := -1;
end;

function TkvDatabaseList.ListRemoveByIndex(const Index: Integer): TkvDatabase;
var
  I, L : Integer;
begin
  L := Length(FList);
  Result := FList[Index];
  for I := Index to L - 2 do
    FList[I] := FList[I + 1];
  SetLength(FList, L - 1);
end;

procedure TkvDatabaseList.OpenNew;
begin
  FFile.OpenNew;
end;

procedure TkvDatabaseList.Open;
var
  RecIdx : Integer;
  Rec : TkvDatabaseListFileRecord;
  Database : TkvDatabase;
begin
  ListClear;
  FFile.Open;
  for RecIdx := 0 to FFile.GetRecordCount - 1 do
    begin
      FFile.LoadRecord(RecIdx, Rec);
      if not (dblfrfDeleted in Rec.Flags) then
        begin
          Database := TkvDatabase.Create(FPath, FSystemName, RecIdx, Rec);
          ListAppend(Database);
          Database.Open;
        end;
    end;
end;

procedure TkvDatabaseList.Close;
var
  I : Integer;
begin
  for I := 0 to Length(FList) - 1 do
    FList[I].Close;
  FFile.Close;
end;

procedure TkvDatabaseList.Delete;
var
  I : Integer;
begin
  for I := 0 to Length(FList) - 1 do
    FList[I].Delete;
  FFile.Delete;
end;

function TkvDatabaseList.GetCount: Integer;
begin
  Result := Length(FList);
end;

function TkvDatabaseList.GetDatabase(const Idx: Integer): TkvDatabase;
begin
  Assert(Idx >= 0);
  Assert(Idx < Length(FList));

  Result := FList[Idx];
end;

function TkvDatabaseList.DatabaseExists(const Name: String): Boolean;
begin
  Result := FHashList.KeyExists(Name);
end;

function TkvDatabaseList.AddDatabase(const Name: String): TkvDatabase;
var
  Rec : TkvDatabaseListFileRecord;
  RecIdx : Word32;
  Database : TkvDatabase;
begin
  Assert(Name <> '');

  kvInitDatabaseListFileRecord(Rec, Name);
  RecIdx := FFile.AppendRecord(Rec);
  Database := TkvDatabase.Create(FPath, FSystemName, RecIdx, Rec);
  Database.OpenNew;
  ListAppend(Database);
  Result := Database;
end;

function TkvDatabaseList.GetDatabaseByName(const Name: String): TkvDatabase;
var
  V : TObject;
begin
  if not FHashList.GetValue(Name, V) then
    Result := nil
  else
    Result := TkvDatabase(V);
end;

function TkvDatabaseList.RequireDatabaseByName(const Name: String): TkvDatabase;
begin
  Result := TkvDatabase(FHashList.RequireValue(Name));
end;

procedure TkvDatabaseList.SaveDatabase(const Database: TkvDatabase);
begin
  Assert(Assigned(Database));
  Assert(Assigned(FFile));

  FFile.SaveRecord(Database.FDatabaseListIdx, Database.FDatabaseListRec);
end;

procedure TkvDatabaseList.Remove(const Name: String);
var
  I : Integer;
  Db : TkvDatabase;
begin
  Assert(Assigned(FFile));

  I := ListGetIndexByName(Name);
  if I < 0 then
    raise EkvObject.CreateFmt('Database not found: %s', [Name]);
  FHashList.DeleteKey(Name);
  Db := ListRemoveByIndex(I);
  Include(Db.FDatabaseListRec.Flags, dblfrfDeleted);
  FFile.SaveRecord(Db.FDatabaseListIdx, Db.FDatabaseListRec);
  Db.Free;
end;



{ TkvSystem }

constructor TkvSystem.Create(const Path, Name: String);
begin
  if Name = '' then
    raise EkvObject.Create('System name required');

  inherited Create;

  FPath := Path;
  FName := Name;

  FOpen := False;
  FSystemFile := TkvSystemFile.Create(Path, Name);
  FSysInfoDataset := TkvDataset.Create(Path, Name, '_sys', 'info');
  FDatabaseList := TkvDatabaseList.Create(Path, Name);
end;

destructor TkvSystem.Destroy;
begin
  FreeAndNil(FDatabaseList);
  FreeAndNil(FSysInfoDataset);
  FreeAndNil(FSystemFile);
  inherited Destroy;
end;

procedure TkvSystem.VerifyNotOpen;
begin
  if FOpen then
    raise EkvObject.Create('Operation not allowed while system is open');
end;

procedure TkvSystem.VerifyOpen;
begin
  if not FOpen then
    raise EkvObject.Create('Operation not allowed while system is closed');
end;

function TkvSystem.Exists: Boolean;
begin
  Result := FSystemFile.Exists;
end;

procedure TkvSystem.OpenNew(const UserDataStr: String);
begin
  VerifyNotOpen;
  if FPath <> '' then
    ForceDirectories(FPath);
  FSystemFile.OpenNew(UserDataStr);
  FSysInfoDataset.OpenNew;
  FDatabaseList.OpenNew;
  FOpen := True;
end;

procedure TkvSystem.Open;
begin
  VerifyNotOpen;
  FSystemFile.Open;
  FSysInfoDataset.Open;
  FDatabaseList.Open;
  FOpen := True;
end;

procedure TkvSystem.Close;
begin
  VerifyOpen;
  FDatabaseList.Close;
  FSysInfoDataset.Close;
  FSystemFile.Close;
  FOpen := False;
end;

procedure TkvSystem.Delete;
begin
  VerifyNotOpen;
  FDatabaseList.Delete;
  FSysInfoDataset.Delete;
  FSystemFile.Delete;
end;

function TkvSystem.GetUserDataStr: String;
begin
  VerifyOpen;
  Result := FSystemFile.UserDataStr;
end;

procedure TkvSystem.SetUserDataStr(const A: String);
begin
  VerifyOpen;
  FSystemFile.UserDataStr := A;
end;

function TkvSystem.AllocateSystemUniqueId: UInt64;
begin
  VerifyOpen;
  Result := FSystemFile.AllocateUniqueId;
end;

function TkvSystem.GetDatabaseCount: Integer;
begin
  Result := FDatabaseList.GetCount;
end;

function TkvSystem.GetDatabaseByIndex(const Idx: Integer): TkvDatabase;
begin
  if (Idx < 0) or (Idx >= FDatabaseList.GetCount) then
    raise EkvObject.Create('Invalid database index');

  Result := FDatabaseList.GetDatabase(Idx);
end;

function TkvSystem.DatabaseExists(const Name: String): Boolean;
begin
  Result := FDatabaseList.DatabaseExists(Name);
end;

function TkvSystem.CreateDatabase(const Name: String): TkvDatabase;
begin
  VerifyOpen;
  if Name = '' then
    raise EkvObject.Create('Database name required');

  if DatabaseExists(Name) then
    raise EkvObject.CreateFmt('Database exists: %s', [Name]);

  Result := FDatabaseList.AddDatabase(Name);
end;

function TkvSystem.RequireDatabaseByName(const Name: String): TkvDatabase;
begin
  Result := FDatabaseList.RequireDatabaseByName(Name);
end;

procedure TkvSystem.DropDatabase(const Name: String);
var
  Db : TkvDatabase;
begin
  VerifyOpen;
  if Name = '' then
    raise EkvObject.Create('Database name required');

  Db := RequireDatabaseByName(Name);
  Db.Close;
  Db.Delete;
  FDatabaseList.Remove(Name);
end;

function TkvSystem.AllocateDatabaseUniqueId(const DatabaseName: String): UInt64;
var
  Db : TkvDatabase;
begin
  VerifyOpen;
  if DatabaseName = '' then
    raise EkvObject.Create('Database name required');

  Db := RequireDatabaseByName(DatabaseName);
  Result := Db.AllocateUniqueId;
  FDatabaseList.SaveDatabase(Db);
end;

function TkvSystem.DatasetExists(const DatabaseName, DatasetName: String): Boolean;
var
  Db : TkvDatabase;
begin
  if DatabaseName = '' then
    raise EkvObject.Create('Database name required');
  if DatasetName = '' then
    raise EkvObject.Create('Dataset name required');

  Db := RequireDatabaseByName(DatabaseName);
  Result := Db.DatasetExists(DatasetName);
end;

function TkvSystem.CreateDataset(const DatabaseName, DatasetName: String): TkvDataset;
var
  Db : TkvDatabase;
  Ds : TkvDataset;
begin
  VerifyOpen;
  if DatabaseName = '' then
    raise EkvObject.Create('Database name required');
  if DatasetName = '' then
    raise EkvObject.Create('Dataset name required');

  Db := RequireDatabaseByName(DatabaseName);
  if Db.DatasetExists(DatasetName) then
    raise EkvObject.CreateFmt('Dataset exists: %s:%s', [DatabaseName, DatasetName]);

  Ds := Db.AddDataset(DatasetName);
  Ds.OpenNew;
  Result := Ds;
end;

function TkvSystem.RequireDatasetByName(const DatabaseName, DatasetName: String): TkvDataset;
var
  Db : TkvDatabase;
begin
  if DatabaseName = '' then
    raise EkvObject.Create('Database name required');
  if DatasetName = '' then
    raise EkvObject.Create('Dataset name required');

  Db := RequireDatabaseByName(DatabaseName);
  Result := Db.RequireDatasetByName(DatasetName);
end;

procedure TkvSystem.DropDataset(const DatabaseName, DatasetName: String);
var
  Db : TkvDatabase;
  Ds : TkvDataset;
begin
  VerifyOpen;
  if DatabaseName = '' then
    raise EkvObject.Create('Database name required');
  if DatasetName = '' then
    raise EkvObject.Create('Dataset name required');

  Db := RequireDatabaseByName(DatabaseName);
  Ds := Db.RequireDatasetByName(DatasetName);
  Ds.Close;
  Ds.Delete;
  Db.RemoveDataset(DatasetName);
end;

function TkvSystem.AllocateDatasetUniqueId(const DatabaseName, DatasetName: String): UInt64;
var
  Db : TkvDatabase;
  Ds : TkvDataset;
begin
  VerifyOpen;
  if DatabaseName = '' then
    raise EkvObject.Create('Database name required');
  if DatasetName = '' then
    raise EkvObject.Create('Dataset name required');

  Db := RequireDatabaseByName(DatabaseName);
  Ds := Db.RequireDatasetByName(DatasetName);
  Result := Ds.AllocateUniqueId;
end;

procedure TkvSystem.AddRecord(const DatabaseName, DatasetName, Key: String;
  const Value: AkvValue);
begin
  VerifyOpen;
  RequireDatasetByName(DatabaseName, DatasetName).AddRecord(Key, Value);
end;

procedure TkvSystem.AddRecord(const Dataset: TkvDataset; const Key: String; const Value: AkvValue);
begin
  VerifyOpen;
  Assert(Assigned(Dataset));
  Dataset.AddRecord(Key, Value);
end;

procedure TkvSystem.MakePath(const DatabaseName, DatasetName, KeyPath: String);
begin
  VerifyOpen;
  RequireDatasetByName(DatabaseName, DatasetName).MakePath(KeyPath);
end;

procedure TkvSystem.MakePath(const Dataset: TkvDataset; const KeyPath: String);
begin
  VerifyOpen;
  Assert(Assigned(Dataset));
  Dataset.MakePath(KeyPath);
end;

function TkvSystem.RecordExists(const DatabaseName, DatasetName, Key: String): Boolean;
begin
  VerifyOpen;
  Result := RequireDatasetByName(DatabaseName, DatasetName).RecordExists(Key);
end;

function TkvSystem.RecordExists(const Dataset: TkvDataset; const Key: String): Boolean;
begin
  VerifyOpen;
  Assert(Assigned(Dataset));
  Result := Dataset.RecordExists(Key);
end;

function TkvSystem.GetRecord(const DatabaseName, DatasetName, Key: String): AkvValue;
begin
  VerifyOpen;
  Result := RequireDatasetByName(DatabaseName, DatasetName).GetRecord(Key);
end;

function TkvSystem.GetRecord(const Dataset: TkvDataset; const Key: String): AkvValue;
begin
  VerifyOpen;
  Assert(Assigned(Dataset));
  Result := Dataset.GetRecord(Key);
end;

procedure TkvSystem.SetRecord(const DatabaseName, DatasetName, Key: String; const Value: AkvValue);
begin
  VerifyOpen;
  RequireDatasetByName(DatabaseName, DatasetName).SetRecord(Key, Value);
end;

procedure TkvSystem.SetRecord(const Dataset: TkvDataset; const Key: String; const Value: AkvValue);
begin
  VerifyOpen;
  Assert(Assigned(Dataset));
  Dataset.SetRecord(Key, Value);
end;

procedure TkvSystem.DeleteRecord(const DatabaseName, DatasetName, Key: String);
begin
  VerifyOpen;
  RequireDatasetByName(DatabaseName, DatasetName).DeleteRecord(Key);
end;

procedure TkvSystem.DeleteRecord(const Dataset: TkvDataset; const Key: String);
begin
  VerifyOpen;
  Assert(Assigned(Dataset));
  Dataset.DeleteRecord(Key);
end;

function TkvSystem.IterateRecords(const DatabaseName, DatasetName: String;
         const Path: String;
         out Iterator: TkvDatasetIterator): Boolean;
begin
  VerifyOpen;
  Result := RequireDatasetByName(DatabaseName, DatasetName).IterateRecords(Path, Iterator);
  Iterator.DatabaseName := DatabaseName;
  Iterator.DatasetName := DatasetName;
end;

function TkvSystem.IterateNextRecord(var Iterator: TkvDatasetIterator): Boolean;
begin
  VerifyOpen;
  Result := RequireDatasetByName(Iterator.DatabaseName, Iterator.DatasetName).IterateNextRecord(Iterator);
end;

function TkvSystem.IteratorGetKey(const Iterator: TkvDatasetIterator): String;
begin
  VerifyOpen;
  Assert(Assigned(Iterator.Dataset));
  Result := Iterator.Dataset.IteratorGetKey(Iterator);
end;

function TkvSystem.IteratorGetValue(const Iterator: TkvDatasetIterator): AkvValue;
begin
  VerifyOpen;
  Assert(Assigned(Iterator.Dataset));
  Result := Iterator.Dataset.IteratorGetValue(Iterator);
end;



end.

