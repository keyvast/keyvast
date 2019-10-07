{ KeyVast - A key value store }
{ Copyright (c) 2018-2019 KeyVast, David J Butler }
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
{ 2018/03/05  0.09  AppendRecord for string and binary}
{ 2018/03/06  0.10  AppendRecord for list and dictionary }
{ 2018/03/07  0.11  Dictionary append improvement }
{ 2018/03/12  0.12  Lists refactor }
{ 2018/04/08  0.13  ListOfKeys }
{ 2018/04/08  0.14  Update Timestamp in hash record }
{ 2018/04/09  0.15  Optional use of Paths in record keys }
{ 2018/04/16  0.16  Set UseFolders on dataset creation }
{ 2018/04/19  0.17  Change Timestamp internal format }
{ 2019/04/19  0.18  Blob record sizes as parameters on dataset creation }
{ 2019/05/20  0.19  Move SysInfoDataset from TkvSystem to TkvScriptSystem }
{ 2019/05/21  0.20  TkvSystem.Backup duplicates system to a new path }
{ 2019/06/10  0.21  Iterate folders }
{ 2019/09/13  0.22  GetRecord and AddRecord support folder values }
{ 2019/09/13  0.23  FolderExists and DeleteFolderRecords }
{ 2019/09/30  0.24  Objects inherit from base objects }
{ 2019/10/03  0.25  Rename to kvDiskSystem }
{ 2019/10/04  0.26  Dataset sets record Timestamp on change }
{ 2019/10/05  0.27  Iterate records with Timestamp filter. }
{ 2019/10/05  0.28  Iterate records non recursive. }
{ 2019/10/05  0.29  Release ParentRecords to DeletedSlots when deleting folder. }
{ 2019/10/05  0.30  Update hash collision children's level hash when hash collision moved down in tree. }
{ 2019/10/05  0.31  Iterate records optionally include folders. }
{ 2019/10/05  0.32  ListOfKeys optionally return record timestamps. }
{ 2019/10/05  0.33  Dataset CopyFrom. }
{ 2019/10/08  0.34  Release KeyValueWithHashCollision children to DeletedSlots when folder deleted. }
{ 2019/10/08  0.35  Add to folder with KeyValueWithHashCollision. }

{$INCLUDE kvInclude.inc}

unit kvDiskSystem;

interface

uses
  System.SysUtils,

  kvHashList,
  kvDiskFileStructures,
  kvDiskFiles,
  kvValues,
  kvAbstractSystem,
  kvBaseSystem;



type
  EkvDiskObject = class(Exception);



{ TkvDataset }

const
  KV_Dataset_KeyBlob_DefaultRecordSize = 128;
  KV_Dataset_ValBlob_DefaultRecordSize = 1024;

type
  TkvDatasetIteratorStackEntry = record
    BaseRecIdx : Word64;
    SlotIdx    : Integer;
    FolderName : String;
  end;
  PkvDatasetIteratorStackEntry = ^TkvDatasetIteratorStackEntry;

  TkvDataset = class;

  TkvDatasetIteratorType = (
      ditRecordsRecursive,
      ditRecordsNonRecursive,
      ditFoldersNonRecursive
    );

  TkvDatasetIterator = class(AkvDatasetIterator)
  private
    FIteratorType   : TkvDatasetIteratorType;
    FIncludeFolders : Boolean;
    FDatabaseName   : String;
    FDatasetName    : String;
    FPath           : String;
    FDataset        : TkvDataset;
    FMinTimestamp   : UInt64;
    FStackLen       : Integer;
    FStack          : array of TkvDatasetIteratorStackEntry;
    FHashRec        : TkvHashFileRecord;

  public
    function  GetDataset: AkvDataset; override;
  end;

  TkvDataset = class(AkvBaseDataset)
  private
    FPath              : String;
    FSystemName        : String;
    FDatabaseName      : String;
    FDatasetListIdx    : Word32;
    FDatasetListRec    : TkvDatasetListFileRecord;
    FName              : String;
    FUseFolders        : Boolean;
    FKeyBlobRecordSize : Word32;
    FValBlobRecordSize : Word32;

    FHashFile  : TkvHashFile;
    FKeyFile   : TkvBlobFile;
    FValueFile : TkvBlobFile;

    function  HashRecSameKey(const HashRec: TkvHashFileRecord;
              const AKey: String; const AKeyHash: UInt64;
              out HashCollision: Boolean): Boolean;
    procedure HashRecSetKey(var HashRec: TkvHashFileRecord;
              const AKey: String; const AKeyHash: UInt64);

    procedure HashRecReleaseValue(var HashRec: TkvHashFileRecord);
    procedure HashRecSetValue(var HashRec: TkvHashFileRecord;
              const AValue: AkvValue);

    procedure HashRecInitKeyValue(out HashRec: TkvHashFileRecord;
              const AKey: String; const AKeyHash: UInt64;
              const AValue: AkvValue;
              const AIsFolder: Boolean; var FolderBaseIdx: Word64);

    procedure RecursiveHashRecSlotCollisionResolve(
              const HashBaseRecIdx, HashRecIdx: Word64;
              var HashRec: TkvHashFileRecord;
              const AKey: String; const AKeyHash: UInt64;
              const AValue: AkvValue;
              const AIsFolder: Boolean; var FolderBaseIdx: Word64;
              const ACollisionLevel: Integer;
              const ATimestampNow: Int64);
    procedure HashRecSlotCollisionResolve(
              const HashBaseRecIdx, HashRecIdx: Word64;
              var HashRec: TkvHashFileRecord;
              const AKey: String; const AKeyHash: UInt64;
              const AValue: AkvValue;
              const AIsFolder: Boolean; var FolderBaseIdx: Word64;
              const ATimestampNow: Int64);

    procedure InternalAddRecord(
              const KeyBaseIdx: Word64; const AKey: String;
              const AValue: AkvValue;
              const AIsFolder: Boolean; out FolderBaseIdx: Word64;
              const ATimestamp: UInt64);
    procedure InternalAddFolderRecords(
              const KeyBaseIdx: Word64;
              const AFolder: TkvFolderValue;
              const ATimestamp: UInt64);
    procedure InternalAddKeyValue(const AKey: String; const AValue: AkvValue;
              const ATimestamp: UInt64);

    function  LocateRecordFromBase(const BaseIndex: Word64; const AKey: String;
              out HashRecIdx: Word64; out HashRec: TkvHashFileRecord): Boolean;
    function  LocateRecord(
              const AKey: String;
              out HashRecIdx: Word64;
              out HashRec: TkvHashFileRecord): Boolean;

    (*procedure UpdateFoldersTimestamp(
              const AKey: String;
              const ATimestamp: UInt64);*)

    function  HashRecToKey(const HashRec: TkvHashFileRecord): String;
    function  HashRecToValue(const HashRec: TkvHashFileRecord): AkvValue;

    procedure GetAllChildRecords(const BaseIndex: Word64; const D: TkvFolderValue);
    function  GetAllFolderRecords(const BaseIndex: Word64): AkvValue;
    function  GetFolderRecord(const HashRec: TkvHashFileRecord): AkvValue;
    function  GetAllRecords: AkvValue;

    procedure HashRecAppendValue_Rewrite(var HashRec: TkvHashFileRecord;
              const AValue: AkvValue);
    procedure HashRecAppendValue_StrOrBin(var HashRec: TkvHashFileRecord;
              const AValue: AkvValue);
    procedure HashRecAppendValue_List(var HashRec: TkvHashFileRecord;
              const AValue: AkvValue);
    procedure HashRecAppendValue_Dictionary(var HashRec: TkvHashFileRecord;
              const AValue: AkvValue);
    procedure HashRecAppendValue(var HashRec: TkvHashFileRecord;
              const AValue: AkvValue);

    procedure ListOfChildKeys(const BaseIdx: Word32; const ADict: TkvDictionaryValue;
              const ARecurse: Boolean; const AIncludeRecordTimestamp: Boolean);
    function  ListOfFolderKeys(const HashRec: TkvHashFileRecord;
              const ARecurse: Boolean; const AIncludeRecordTimestamp: Boolean): TkvDictionaryValue;
    function  ListOfRootKeys(const ARecurse: Boolean; const AIncludeRecordTimestamp: Boolean): TkvDictionaryValue;

    procedure InternalDeleteKeyValue(const HashRecIdx: Word64;
              var HashRec: TkvHashFileRecord);
    procedure InternalDeleteChildren(const BaseIndex: Word64; const AUpdateRecords: Boolean);
    procedure InternalDeleteRecord(const HashRecIdx: Word64;
              var HashRec: TkvHashFileRecord);

    function  SetNextIteratorRecord(var AIterator: TkvDatasetIterator): Boolean;
    function  IterateFirst(const AIteratorType: TkvDatasetIteratorType;
              const APath: String;
              out AIterator: AkvDatasetIterator;
              const AIncludeFolders: Boolean;
              const AMinTimestamp: UInt64): Boolean;

  protected
    function  GetName: String; override;
    function  GetUseFolders: Boolean; override;

  public
    constructor Create(
                const APath, ASystemName, ADatabaseName: String;
                const ADatasetListIdx: Word32;
                const ADatasetListRec: TkvDatasetListFileRecord;
                const AHashFileCacheEntries: Word32 = KV_HashFile_DefaultCacheEntries
                ); overload;
    constructor Create(
                const APath, ASystemName, ADatabaseName, ADatasetName: String;
                const AUseFolders: Boolean;
                const AKeyBlobRecordSize: Word32 = KV_Dataset_KeyBlob_DefaultRecordSize;
                const AValBlobRecordSize: Word32 = KV_Dataset_ValBlob_DefaultRecordSize;
                const AHashFileCacheEntries: Word32 = KV_HashFile_DefaultCacheEntries
                ); overload;
    destructor Destroy; override;
    procedure Finalise;

    property  Name: String read FName;
    property  UseFolders: Boolean read FUseFolders;

    property  KeyBlobRecordSize: Word32 read FKeyBlobRecordSize;
    property  ValBlobRecordSize: Word32 read FValBlobRecordSize;

    procedure OpenNew;
    procedure Open;
    procedure Close;
    procedure Delete;

    function  GetTimestamp: UInt64; override;

    function  GetUniqueId: UInt64; override;
    function  AllocateUniqueId: UInt64; override;

    function  RecordExists(const AKey: String): Boolean; override;
    function  GetRecordIfExists(const AKey: String): AkvValue; override;
    function  GetRecord(const AKey: String): AkvValue; override;
    function  FolderExists(const AKey: String): Boolean; override;
    procedure MakePath(const AKeyPath: String); override;
    procedure AddRecord(const AKey: String; const AValue: AkvValue); override;
    procedure SetRecord(const AKey: String; const AValue: AkvValue); override;
    procedure SetOrAddRecord(const AKey: String; const AValue: AkvValue); override;
    procedure AppendRecord(const AKey: String; const AValue: AkvValue); override;
    procedure DeleteRecord(const AKey: String); override;
    procedure DeleteFolderRecords(const APath: String); override;

    function  ListOfKeys(
              const AKeyPath: String;
              const ARecurse: Boolean;
              const AIncludeRecordTimestamp: Boolean = False): TkvDictionaryValue; override;
    function  IterateRecords(const APath: String; out AIterator: AkvDatasetIterator;
              const ARecurse: Boolean = True;
              const AIncludeFolders: Boolean = False;
              const AMinTimestamp: UInt64 = 0): Boolean; override;
    function  IterateFolders(const APath: String; out AIterator: AkvDatasetIterator): Boolean; override;
    function  IterateNextRecord(var AIterator: AkvDatasetIterator): Boolean; override;
    function  IteratorHasRecord(const AIterator: AkvDatasetIterator): Boolean; override;
    function  IteratorIsFolder(const AIterator: AkvDatasetIterator): Boolean; override;
    function  IteratorGetKey(const AIterator: AkvDatasetIterator): String; override;
    function  IteratorGetValue(const AIterator: AkvDatasetIterator): AkvValue; override;
    function  IteratorGetTimestamp(const AIterator: AkvDatasetIterator): UInt64; override;

    procedure CopyFrom(const ADataset: AkvDataset); override;

    procedure BackupTo(const ADataset: TkvDataset);
  end;



  { TkvDatasetList }

  TkvDatabase = class;

  TkvDatasetListIterator = class(AkvDatasetListIterator)
  private
    FDatabase : TkvDatabase;
    FIterator : TkvStringHashListIterator;
    FItem     : PkvStringHashListItem;
    FKey      : String;
    FDataset  : TkvDataset;

  public
    function  GetDatabase: AkvDatabase; override;
    function  GetName: String; override;
    function  GetDataset: AkvDataset; override;
  end;

  TkvDatasetList = class
  private
    FPath         : String;
    FSystemName   : String;
    FDatabaseName : String;

    FFile : TkvDatasetListFile;
    FList : TkvStringHashList;

    procedure ListClear;
    procedure ListAppend(const AItem: TkvDataset);
    function  IterateFirst(var AIterator: AkvDatasetListIterator): Boolean;
    function  IterateNext(var AIterator: AkvDatasetListIterator): Boolean;

  public
    constructor Create(const APath, ASystemName, ADatabaseName: String);
    destructor Destroy; override;
    procedure Finalise;

    procedure OpenNew;
    procedure Open(const AHashFileCacheEntries: Word32 = KV_HashFile_DefaultCacheEntries);
    procedure Close;
    procedure Delete;

    function  GetCount: Integer;

    function  Exists(const AName: String): Boolean;
    function  Add(const AName: String;
                  const AUseFolders: Boolean;
                  const AKeyBlobRecordSize: Word32 = KV_Dataset_KeyBlob_DefaultRecordSize;
                  const AValBlobRecordSize: Word32 = KV_Dataset_ValBlob_DefaultRecordSize;
                  const AHashFileCacheEntries: Word32 = KV_HashFile_DefaultCacheEntries
                  ): TkvDataset;

    function  RequireItemByName(const AName: String): TkvDataset;

    procedure Remove(const AName: String);

    procedure SaveDataset(const ADataset: TkvDataset);
  end;



  TkvSystem = class;

  { TkvDatabase }

  TkvDatabase = class(AkvBaseDatabase)
  private
    FSystem          : TkvSystem;
    FPath            : String;
    FDatabaseListIdx : Word32;
    FDatabaseListRec : TkvDatabaseListFileRecord;

    FName        : String;
    FDatasetList : TkvDatasetList;

  protected
    function  GetName: String; override;

  public
    constructor Create(
                const ASystem: TkvSystem;
                const APath, ASystemName: String;
                const ADatabaseListIdx: Word32;
                const ADatabaseListRec: TkvDatabaseListFileRecord);
    destructor Destroy; override;
    procedure Finalise;

    property  Name: String read FName;

    function  AllocateUniqueId: UInt64; override;

    procedure OpenNew;
    procedure Open(const AHashFileCacheEntries: Word32 = KV_HashFile_DefaultCacheEntries);
    procedure Close;
    procedure Delete;

    property  DatasetList: TkvDatasetList read FDatasetList;

    function  GetDatasetCount: Integer; override;
    function  IterateFirstDataset(var AIterator: AkvDatasetListIterator): Boolean; override;
    function  IterateNextDataset(var AIterator: AkvDatasetListIterator): Boolean; override;

    function  RequireDatasetByName(const AName: String): AkvDataset; override;
    function  DatasetExists(const AName: String): Boolean; override;
    function  AddDiskDataset(
              const AName: String;
              const AUseFolders: Boolean;
              const AKeyBlobRecordSize: Word32 = KV_Dataset_KeyBlob_DefaultRecordSize;
              const AValBlobRecordSize: Word32 = KV_Dataset_ValBlob_DefaultRecordSize;
              const AHashFileCacheEntries: Word32 = KV_HashFile_DefaultCacheEntries
              ): TkvDataset;
    function  AddDataset(const AName: String; const AUseFolders: Boolean): AkvDataset; override;
    procedure RemoveDataset(const AName: String); override;
  end;



  { TkvDatabaseList }

  TkvDatabaseListIterator = class(AkvDatabaseListIterator)
  private
    FIterator : TkvStringHashListIterator;
    FItem     : PkvStringHashListItem;
    FKey      : String;
    FDatabase : TkvDatabase;

  public
    function  GetName: String; override;
    function  GetDatabase: AkvDatabase; override;
  end;

  TkvDatabaseList = class
  private
    FSystem     : TkvSystem;
    FPath       : String;
    FSystemName : String;

    FFile : TkvDatabaseListFile;
    FList : TkvStringHashList;

    procedure ListClear;
    procedure ListAppend(const AItem: TkvDatabase);

  public
    constructor Create(const ASystem: TkvSystem; const APath, ASystemName: String);
    destructor Destroy; override;
    procedure Finalise;

    procedure OpenNew;
    procedure Open;
    procedure Close;
    procedure Delete;

    function  GetCount: Integer;
    function  IterateFirst(var AIterator: AkvDatabaseListIterator): Boolean;
    function  IterateNext(var AIterator: AkvDatabaseListIterator): Boolean;

    function  DatabaseExists(const AName: String): Boolean;
    function  AddDatabase(const AName: String): TkvDatabase;

    function  GetDatabaseByName(const AName: String): TkvDatabase;
    function  RequireDatabaseByName(const AName: String): TkvDatabase;

    procedure SaveDatabase(const ADatabase: TkvDatabase);

    procedure Remove(const AName: String);
  end;



  { TkvSystem }

  TkvSystem = class(AkvBaseSystem)
  private
    FPath : String;
    FName : String;

    FOpen         : Boolean;
    FSystemFile   : TkvSystemFile;
    FDatabaseList : TkvDatabaseList;

    procedure VerifyNotOpen;
    procedure VerifyOpen;

  protected
    function  GetUserDataStr: String; override;
    procedure SetUserDataStr(const AUserDataStr: String); override;

  public
    constructor Create(const APath, AName: String);
    destructor Destroy; override;
    procedure Finalise;

    function  Exists: Boolean;
    procedure OpenNew(const AUserDataStr: String = '');
    procedure Open;
    procedure Close;
    procedure Delete;

    property  UserDataStr: String read GetUserDataStr write SetUserDataStr;

    function  AllocateUniqueId: UInt64; override;

    function  DatabaseExists(const AName: String): Boolean; override;
    function  CreateDatabase(const AName: String): AkvDatabase; override;
    function  RequireDatabaseByName(const AName: String): AkvDatabase; override;
    procedure DropDatabase(const AName: String); override;
    function  GetDatabaseCount: Integer; override;
    function  IterateFirstDatabase(var AIterator: AkvDatabaseListIterator): Boolean; override;
    function  IterateNextDatabase(var AIterator: AkvDatabaseListIterator): Boolean; override;

    function  CreateDiskDataset(
              const ADatabaseName, ADatasetName: String;
              const AUseFolders: Boolean;
              const AKeyBlobRecordSize: Word32 = KV_Dataset_KeyBlob_DefaultRecordSize;
              const AValBlobRecordSize: Word32 = KV_Dataset_ValBlob_DefaultRecordSize;
              const AHashFileCacheEntries: Word32 = KV_HashFile_DefaultCacheEntries): TkvDataset;

    function  Backup(
              const ABackupPath: String;
              const AHashFileCacheEntries: Word32 = KV_HashFile_DefaultCacheEntries): TkvSystem;
  end;



implementation

uses
  kvDiskHash;



{ Helper functions }

procedure kvNameFromBuf(var S: String; const Buf; const Len: Integer);
begin
  SetLength(S, Len);
  if Len > 0 then
    Move(Buf, PChar(S)^, Len * SizeOf(Char));
end;



{ TkvDatasetIterator }

function TkvDatasetIterator.GetDataset: AkvDataset;
begin
  Result := FDataset;
end;



{ TkvDataset }

constructor TkvDataset.Create(
            const APath, ASystemName, ADatabaseName: String;
            const ADatasetListIdx: Word32;
            const ADatasetListRec: TkvDatasetListFileRecord;
            const AHashFileCacheEntries: Word32);
begin
  Assert(ASystemName <> '');
  Assert(ADatabaseName <> '');

  inherited Create;

  FPath := APath;
  FSystemName := ASystemName;
  FDatabaseName := ADatabaseName;
  FDatasetListIdx := ADatasetListIdx;
  FDatasetListRec := ADatasetListRec;

  kvNameFromBuf(FName, FDatasetListRec.Name[0], FDatasetListRec.NameLength);
  FUseFolders := FDatasetListRec.UseFolders;
  FKeyBlobRecordSize := FDatasetListRec.KeyBlobRecordSize;
  FValBlobRecordSize := FDatasetListRec.ValBlobRecordSize;

  FHashFile := TkvHashFile.Create(FPath, FSystemName, FDatabaseName, FName, AHashFileCacheEntries);
  FKeyFile := TkvBlobFile.Create(FPath, FSystemName, FDatabaseName, FName, 'k');
  FValueFile := TkvBlobFile.Create(FPath, FSystemName, FDatabaseName, FName, 'v');
end;

constructor TkvDataset.Create(const APath, ASystemName, ADatabaseName, ADatasetName: String;
            const AUseFolders: Boolean;
            const AKeyBlobRecordSize: Word32;
            const AValBlobRecordSize: Word32;
            const AHashFileCacheEntries: Word32);
begin
  Assert(ASystemName <> '');
  Assert(ADatabaseName <> '');
  Assert(ADatasetName <> '');

  inherited Create;

  FPath := APath;
  FSystemName := ASystemName;
  FDatabaseName := ADatabaseName;
  FName := ADatasetName;
  FUseFolders := AUseFolders;

  kvValidateBlobFileRecordSize(AKeyBlobRecordSize);
  FKeyBlobRecordSize := AKeyBlobRecordSize;

  kvValidateBlobFileRecordSize(AValBlobRecordSize);
  FValBlobRecordSize := AValBlobRecordSize;

  FHashFile := TkvHashFile.Create(FPath, FSystemName, FDatabaseName, FName, AHashFileCacheEntries);
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

procedure TkvDataset.Finalise;
begin
  if Assigned(FValueFile) then
    FValueFile.Finalise;
  if Assigned(FKeyFile) then
    FKeyFile.Finalise;
  if Assigned(FHashFile) then
    FHashFile.Finalise;
end;

function TkvDataset.GetName: String;
begin
  Result := FName;
end;

function TkvDataset.GetUseFolders: Boolean;
begin
  Result := FUseFolders;
end;

procedure TkvDataset.OpenNew;
begin
  FHashFile.OpenNew;
  FKeyFile.OpenNew(FKeyBlobRecordSize);
  FValueFile.OpenNew(FValBlobRecordSize);
end;

procedure TkvDataset.Open;
begin
  FHashFile.Open;
  FKeyFile.Open;
  FValueFile.Open;
end;

procedure TkvDataset.CopyFrom(const ADataset: AkvDataset);
var
  Hdr : PkvHashFileHeader;
  DsIt : AkvDatasetIterator;
  Key : String;
  Timestamp : UInt64;
  Val : AkvValue;
begin
  DeleteFolderRecords('/');
  Hdr := FHashFile.GetHeader;
  Hdr^.UniqueIdCounter := ADataset.GetUniqueId;
  Hdr^.TimestampCounter := ADataset.GetTimestamp;
  FHashFile.HeaderModified;
  if ADataset.IterateRecords('', DsIt, True, False, 0) then
    repeat
      Key := ADataset.IteratorGetKey(DsIt);
      Val := ADataset.IteratorGetValue(DsIt);
      try
        Timestamp := ADataset.IteratorGetTimestamp(DsIt);
        if (Timestamp = 0) or (Timestamp > Hdr^.TimestampCounter) then
          // Upgrade to counter based timestamp
          Timestamp := Hdr^.TimestampCounter;
        InternalAddKeyValue(Key, Val, Timestamp);
      finally
        FreeAndNil(Val);
      end;
    until not ADataset.IterateNextRecord(DsIt);
  FHashFile.UpdateHeader;
end;

procedure TkvDataset.BackupTo(const ADataset: TkvDataset);
var
  Hdr : PkvHashFileHeader;
  BakHdr : PkvHashFileHeader;
  DsIt : AkvDatasetIterator;
  Key : String;
  Timestamp : UInt64;
  Val : AkvValue;
begin
  //// ADataset.CopyFrom(self)
  Hdr := FHashFile.GetHeader;
  BakHdr := ADataset.FHashFile.GetHeader;
  BakHdr^.UniqueIdCounter := Hdr.UniqueIdCounter;
  BakHdr^.TimestampCounter := Hdr.TimestampCounter;
  ADataset.FHashFile.HeaderModified;
  if IterateRecords('', DsIt, True, False, 0) then
    repeat
      Key := IteratorGetKey(DsIt);
      Val := IteratorGetValue(DsIt);
      try
        Timestamp := IteratorGetTimestamp(DsIt);
        if (Timestamp = 0) or (Timestamp > Hdr.TimestampCounter) then
          // Upgrade to counter based timestamp
          Timestamp := Hdr.TimestampCounter;
        ADataset.InternalAddKeyValue(Key, Val, Timestamp);
      finally
        FreeAndNil(Val);
      end;
    until not IterateNextRecord(DsIt);
  ADataset.FHashFile.UpdateHeader;
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

function TkvDataset.GetTimestamp: UInt64;
begin
  Result := FHashFile.GetTimestamp;
end;

function TkvDataset.GetUniqueId: UInt64;
begin
  Result := FHashFile.GetHeader^.UniqueIdCounter;
end;

function TkvDataset.AllocateUniqueId: UInt64;
begin
  Result := FHashFile.AllocateUniqueId;
  FHashFile.UpdateHeader;
end;

// Returns True if HashRec's key matches Key
// HashCollision is set True if keys are different but have same hash
function TkvDataset.HashRecSameKey(const HashRec: TkvHashFileRecord;
         const AKey: String; const AKeyHash: UInt64;
         out HashCollision: Boolean): Boolean;
var
  KeyLen : Word32;
  R : Boolean;
  N, I : Integer;
begin
  HashCollision := False;
  if HashRec.KeyHash = AKeyHash then
    begin
      KeyLen := Length(AKey);
      Assert(KeyLen <= KV_HashFile_MaxKeyLength);
      if HashRec.KeyLength = KeyLen then
        begin
          R := True;
          N := KeyLen;
          if N > KV_HashFileRecord_SlotShortKeyLength then
            N := KV_HashFileRecord_SlotShortKeyLength;
          for I := 0 to N - 1 do
            if HashRec.KeyShort[I] <> AKey[I + 1] then
              begin
                R := False;
                break;
              end;
          if R then
            if KeyLen > KV_HashFileRecord_SlotShortKeyLength then
              if HashRecToKey(HashRec) <> AKey then
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
          const AKey: String; const AKeyHash: UInt64);
var
  KeyLen : Word32;
  N : Integer;
begin
  KeyLen := Length(AKey);
  Assert(KeyLen <= KV_HashFile_MaxKeyLength);
  HashRec.KeyHash := AKeyHash;
  HashRec.KeyLength := KeyLen;
  N := KeyLen;
  if N > KV_HashFileRecord_SlotShortKeyLength then
    N := KV_HashFileRecord_SlotShortKeyLength;
  Move(PChar(AKey)^, HashRec.KeyShort[0], N * SizeOf(Char));
  if KeyLen > KV_HashFileRecord_SlotShortKeyLength then
    HashRec.KeyLongChainIndex := FKeyFile.CreateChain(PChar(AKey)^, KeyLen * SizeOf(Char))
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
          const AValue: AkvValue);
var
  M : Integer;
  ValBuf : Pointer;
begin
  M := AValue.SerialSize;
  if M <= KV_HashFileRecord_SlotShortValueSize then
    begin
      HashRecReleaseValue(HashRec);
      HashRec.ValueType := hfrvtShort;
      AValue.GetSerialBuf(HashRec.ValueShort[0], KV_HashFileRecord_SlotShortValueSize);
    end
  else
    begin
      GetMem(ValBuf, M);
      try
        AValue.GetSerialBuf(ValBuf^, M);
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
  HashRec.ValueTypeId := AValue.TypeId;
end;

// Initialises Key and Value in HashRec, allocating folder slots if Folder
procedure TkvDataset.HashRecInitKeyValue(out HashRec: TkvHashFileRecord;
          const AKey: String; const AKeyHash: UInt64;
          const AValue: AkvValue;
          const AIsFolder: Boolean; var FolderBaseIdx: Word64);
begin
  kvInitHashFileRecord(HashRec);
  HashRec.RecordType := hfrtKeyValue;
  if AIsFolder then
    begin
      HashRec.ValueType := hfrvtFolder;
      HashRec.ValueFolderBaseIndex := FHashFile.AllocateSlotRecords;
      FolderBaseIdx := HashRec.ValueFolderBaseIndex;
    end
  else
    HashRecSetValue(HashRec, AValue);
  HashRecSetKey(HashRec, AKey, AKeyHash);
end;

const
  KV_HashFile_MaxSlotCollisionCount = 6;

// Resolves slot collision between HashRec and Key
// HashBaseRecIdx and HashRecIdx is the base index and record index of HashRec
// When resolved new slot records with separate slot entries for HashRec and Key/Value is populated
procedure TkvDataset.RecursiveHashRecSlotCollisionResolve(
          const HashBaseRecIdx, HashRecIdx: Word64; var HashRec: TkvHashFileRecord;
          const AKey: String; const AKeyHash: UInt64;
          const AValue: AkvValue;
          const AIsFolder: Boolean; var FolderBaseIdx: Word64;
          const ACollisionLevel: Integer;
          const ATimestampNow: Int64);
var
  BaseIdx : Word64;
  RecIdx : Word64;
  Key1Hash : UInt64;
  Key2Hash : UInt64;
  Slt1 : Word32;
  Slt2 : Word32;
  ParentHashRec : TkvHashFileRecord;
  NewHashRec : TkvHashFileRecord;
  ColBaseIdx : Word64;
  ColIdx : Word64;
  ColRec : TkvHashFileRecord;
  I : Integer;
begin
  if ACollisionLevel > KV_HashFile_MaxSlotCollisionCount then
    raise EkvDiskObject.Create('Hash failure: Too many slot collisions');

  Key1Hash := AKeyHash;
  Key2Hash := HashRec.KeyHash;
  if Key1Hash = Key2Hash then
    // Should not happen since previous level hash had no collision and
    // kvLevelNHash only reorders bits, so next level hash should not collide.
    raise EkvDiskObject.Create('Hash failure: Hash collision');

  BaseIdx := FHashFile.AllocateSlotRecords;

  kvInitHashFileRecord(ParentHashRec);
  ParentHashRec.RecordType := hfrtParentSlot;
  ParentHashRec.ChildSlotRecordIndex := BaseIdx;

  // Calculate hash for this level
  Key1Hash := kvLevelNHash(Key1Hash);
  Key2Hash := kvLevelNHash(Key2Hash);

  HashRec.KeyHash := Key2Hash;

  Slt1 := Key1Hash mod KV_HashFile_LevelSlotCount;
  Slt2 := Key2Hash mod KV_HashFile_LevelSlotCount;
  if Slt1 = Slt2 then
    begin
      // Slot collision on this level, recurse down a level
      RecIdx := BaseIdx + Slt1;
      RecursiveHashRecSlotCollisionResolve(BaseIdx, RecIdx, HashRec,
          AKey, Key1Hash, AValue, AIsFolder, FolderBaseIdx, ACollisionLevel + 1, ATimestampNow);
    end
  else
    begin
      // Save existing record in new level/slot without a slot collision
      FHashFile.SaveRecord(BaseIdx + Slt2, HashRec);
      if HashRec.RecordType = hfrtKeyValueWithHashCollision then
        begin
          // Update all hash collision children's level hash with parent's level hash
          ColBaseIdx := HashRec.ChildSlotRecordIndex;
          for I := 0 to KV_HashFile_LevelSlotCount - 1 do
            begin
              ColIdx := ColBaseIdx + Word32(I);
              FHashFile.LoadRecord(ColIdx, ColRec);
              if ColRec.RecordType in [hfrtKeyValue] then
                begin
                  ColRec.KeyHash := HashRec.KeyHash;
                  FHashFile.SaveRecord(ColIdx, ColRec);
                end;
            end;
        end;
      // Save new record
      HashRecInitKeyValue(NewHashRec, AKey, Key1Hash, AValue, AIsFolder, FolderBaseIdx);
      NewHashRec.Timestamp := ATimestampNow;
      FHashFile.SaveRecord(BaseIdx + Slt1, NewHashRec);
    end;
  // Save parent record
  FHashFile.SaveRecord(HashRecIdx, ParentHashRec);
end;

procedure TkvDataset.HashRecSlotCollisionResolve(
          const HashBaseRecIdx, HashRecIdx: Word64; var HashRec: TkvHashFileRecord;
          const AKey: String; const AKeyHash: UInt64;
          const AValue: AkvValue;
          const AIsFolder: Boolean; var FolderBaseIdx: Word64;
          const ATimestampNow: Int64);
begin
  Assert(HashRec.RecordType in [hfrtKeyValue, hfrtKeyValueWithHashCollision]);

  RecursiveHashRecSlotCollisionResolve(HashBaseRecIdx, HashRecIdx, HashRec,
      AKey, AKeyHash, AValue, AIsFolder, FolderBaseIdx, 1, ATimestampNow);

  FHashFile.HeaderModified;
end;

procedure TkvDataset.InternalAddRecord(
          const KeyBaseIdx: Word64; const AKey: String;
          const AValue: AkvValue;
          const AIsFolder: Boolean; out FolderBaseIdx: Word64;
          const ATimestamp: UInt64);
var
  Hsh : UInt64;
  SltI : Word32;
  HashRecBaseI : Word64;
  HashRecI : Word64;
  HashRec : TkvHashFileRecord;
  NewHashRec : TkvHashFileRecord;
  HashCol : Boolean;
  RecSl : Word32;
  RecI, EmpI : Word64;
  Fin : Boolean;
begin
  FolderBaseIdx := KV_HashFile_InvalidIndex;
  Hsh := kvLevel1HashString(AKey, True);
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
          HashRecInitKeyValue(HashRec, AKey, Hsh, AValue, AIsFolder, FolderBaseIdx);
          HashRec.Timestamp := ATimestamp;
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
          if HashRecSameKey(HashRec, AKey, Hsh, HashCol) then
            if AIsFolder and (HashRec.ValueType = hfrvtFolder) then
              begin
                FolderBaseIdx := HashRec.ValueFolderBaseIndex;
                exit;
              end
            else
              raise EkvDiskObject.CreateFmt('Key exists: %s', [AKey]);
          if HashCol then
            begin
              // Change key/value entry to key/value-with-hash-collision entry
              HashRec.RecordType := hfrtKeyValueWithHashCollision;
              HashRec.ChildSlotRecordIndex := FHashFile.AllocateSlotRecords;
              FHashFile.SaveRecord(HashRecI, HashRec);
              // Save new key/value to child slot 0
              HashRecInitKeyValue(NewHashRec, AKey, Hsh, AValue, AIsFolder, FolderBaseIdx);
              NewHashRec.Timestamp := ATimestamp;
              FHashFile.SaveRecord(HashRec.ChildSlotRecordIndex, NewHashRec);
            end
          else
            HashRecSlotCollisionResolve(HashRecBaseI, HashRecI, HashRec,
                AKey, Hsh, AValue, AIsFolder, FolderBaseIdx, ATimestamp);
          Fin := True;
        end;
      hfrtKeyValueWithHashCollision :
        begin
          // Check this key/value entry for duplicate key
          if HashRecSameKey(HashRec, AKey, Hsh, HashCol) then
            if AIsFolder and (HashRec.ValueType = hfrvtFolder) then
              begin
                FolderBaseIdx := HashRec.ValueFolderBaseIndex;
                exit;
              end
            else
              raise EkvDiskObject.CreateFmt('Key exists: %s', [AKey]);
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
                      if HashRecSameKey(NewHashRec, AKey, Hsh, HashCol) then
                        if AIsFolder and (NewHashRec.ValueType = hfrvtFolder) then
                          begin
                            FolderBaseIdx := NewHashRec.ValueFolderBaseIndex;
                            exit;
                          end
                        else
                          raise EkvDiskObject.CreateFmt('Key exists: %s', [AKey]);
                    hfrtEmpty :
                      if EmpI = KV_HashFile_InvalidIndex then
                        EmpI := RecI;
                  end;
                end;
              if EmpI = KV_HashFile_InvalidIndex then
                // no more empty slots left in hash collision slots
                raise EkvDiskObject.Create('Hash failure: Too many hash collisions');
              // Replace empty child slot with new key/value entry
              HashRecInitKeyValue(NewHashRec, AKey, Hsh, AValue, AIsFolder, FolderBaseIdx);
              NewHashRec.Timestamp := ATimestamp;
              FHashFile.SaveRecord(EmpI, NewHashRec);
            end
          else
            // HashRec and its child collisions are moved as one
            HashRecSlotCollisionResolve(HashRecBaseI, HashRecI, HashRec,
                AKey, Hsh, AValue, AIsFolder, FolderBaseIdx, ATimestamp);
          Fin := True;
        end;
    else
      raise EkvDiskObject.Create('Invalid hash record type');
    end;
  until Fin;
end;

procedure TkvDataset.InternalAddFolderRecords(
          const KeyBaseIdx: Word64;
          const AFolder: TkvFolderValue;
          const ATimestamp: UInt64);
var
  Iter : TkvDictionaryValueIterator;
  Key : String;
  Value : AkvValue;
  BaseIdx : Word64;
begin
  Assert(Assigned(AFolder));

  if AFolder.IterateFirst(Iter) then
    repeat
      AFolder.IteratorGetKeyValue(Iter, Key, Value);
      Assert(Key <> '');
      Assert(Assigned(Value));
      if Value is TkvFolderValue then
        begin
          InternalAddRecord(KeyBaseIdx, Key, nil, True, BaseIdx, ATimestamp);
          InternalAddFolderRecords(BaseIdx, TkvFolderValue(Value), ATimestamp);
        end
      else
        InternalAddRecord(KeyBaseIdx, Key, Value, False, BaseIdx, ATimestamp);
    until not AFolder.IterateNext(Iter);
end;

procedure TkvDataset.InternalAddKeyValue(const AKey: String; const AValue: AkvValue;
          const ATimestamp: UInt64);
var
  KeyLen : Integer;
  BaseIdx : Word64;
  StartI : Integer;
  FolderSepI : Integer;
  SubKey : String;
begin
  KeyLen := Length(AKey);

  Assert(KeyLen > 0);
  Assert(KeyLen <= KV_HashFile_MaxKeyLength);
  Assert(not FUseFolders or not AKey.EndsWith('/'));
  Assert(Assigned(AValue));

  if not FUseFolders then
    begin
      BaseIdx := 0;
      SubKey := AKey;
      InternalAddRecord(BaseIdx, SubKey, AValue, False, BaseIdx, ATimestamp);
    end
  else
    begin
      BaseIdx := 0;
      StartI := 0;
      repeat
        FolderSepI := AKey.IndexOf(Char('/'), StartI);
        if FolderSepI = 0 then
          raise EkvDiskObject.Create('Invalid key: Empty folder name');
        if FolderSepI > 0 then
          begin
            SubKey := Copy(AKey, StartI + 1, FolderSepI - StartI);
            InternalAddRecord(BaseIdx, SubKey, nil, True, BaseIdx, ATimestamp);
            StartI := FolderSepI + 1;
          end;
      until FolderSepI < 0;

      if StartI = 0 then
        SubKey := AKey
      else
        SubKey := Copy(AKey, StartI + 1, KeyLen - StartI);

      if AValue is TkvFolderValue then
        begin
          InternalAddRecord(BaseIdx, SubKey, nil, True, BaseIdx, ATimestamp);
          InternalAddFolderRecords(BaseIdx, TkvFolderValue(AValue), ATimestamp);
        end
      else
        InternalAddRecord(BaseIdx, SubKey, AValue, False, BaseIdx, ATimestamp);
    end;
end;

const
  KV_Dataset_LocateMaxLevels = 128;

function TkvDataset.LocateRecordFromBase(
         const BaseIndex: Word64;
         const AKey: String;
         out HashRecIdx: Word64;
         out HashRec: TkvHashFileRecord): Boolean;
var
  Hsh : UInt64;
  SltI : Word32;
  HashRecBaseI : Word64;
  HashRecI : Word64;
  HashCol : Boolean;
  Level : Integer;
  R, Fin : Boolean;
begin
  Assert(AKey <> '');

  Level := 0;
  R := False;
  Hsh := kvLevel1HashString(AKey, True);
  SltI := Hsh mod KV_HashFile_LevelSlotCount;
  HashRecBaseI := BaseIndex;
  Fin := False;
  repeat
    Inc(Level);
    if Level = KV_Dataset_LocateMaxLevels then
      raise EkvDiskObject.Create('Key too deep');
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
          R := HashRecSameKey(HashRec, AKey, Hsh, HashCol);
          if R then
            HashRecIdx := HashRecI;
          Fin := True;
        end;
      hfrtKeyValueWithHashCollision :
        begin
          // Check key/value entry first
          R := HashRecSameKey(HashRec, AKey, Hsh, HashCol);
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
                  R := HashRec.RecordType = hfrtKeyValue;
                  if R then
                    R := HashRecSameKey(HashRec, AKey, Hsh, HashCol);
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
      raise EkvDiskObject.Create('Invalid hash record type');
    end;
  until Fin;
  if not R then
    HashRecIdx := $FFFFFFFF;
  Result := R;
end;

function TkvDataset.LocateRecord(
         const AKey: String;
         out HashRecIdx: Word64;
         out HashRec: TkvHashFileRecord): Boolean;
var
  KeyLen : Integer;
  BaseIdx : Word64;
  StartI : Integer;
  FolderSepI : Integer;
  SubKey : String;
begin
  if not FUseFolders then
    Result := LocateRecordFromBase(0, AKey, HashRecIdx, HashRec)
  else
    begin
      KeyLen := Length(AKey);
      BaseIdx := 0;
      StartI := 0;
      repeat
        FolderSepI := AKey.IndexOf(Char('/'), StartI);
        if FolderSepI = 0 then
          raise EkvDiskObject.Create('Invalid key: Empty folder name');
        if FolderSepI > 0 then
          begin
            SubKey := Copy(AKey, StartI + 1, FolderSepI - StartI);
            Result := LocateRecordFromBase(BaseIdx, SubKey, HashRecIdx, HashRec);
            if not Result then
              exit;
            if HashRec.ValueType <> hfrvtFolder then
              raise EkvDiskObject.Create('Invalid key: Not a folder');
            BaseIdx := HashRec.ValueFolderBaseIndex;
            StartI := FolderSepI + 1;
          end;
      until FolderSepI < 0;

      if StartI = 0 then
        SubKey := AKey
      else
        SubKey := Copy(AKey, StartI + 1, KeyLen - StartI);

      Result := LocateRecordFromBase(BaseIdx, SubKey, HashRecIdx, HashRec);
    end;
end;

(*
procedure TkvDataset.UpdateFoldersTimestamp(
          const AKey: String;
          const ATimestamp: UInt64);
var
  BaseIdx : Word64;
  StartI : Integer;
  FolderSepI : Integer;
  SubKey : String;
  HashRecIdx : Word64;
  HashRec : TkvHashFileRecord;
begin
  if not FUseFolders then
    exit;
  BaseIdx := 0;
  StartI := 0;
  repeat
    FolderSepI := AKey.IndexOf(Char('/'), StartI);
    if FolderSepI = 0 then
      exit;
    if FolderSepI > 0 then
      begin
        SubKey := Copy(AKey, StartI + 1, FolderSepI - StartI);
        if not LocateRecordFromBase(BaseIdx, SubKey, HashRecIdx, HashRec) then
          exit;
        if HashRec.ValueType <> hfrvtFolder then
          exit;
        HashRec.Timestamp := ATimestamp;
        FHashFile.SaveRecord(HashRecIdx, HashRec);
        BaseIdx := HashRec.ValueFolderBaseIndex;
        StartI := FolderSepI + 1;
      end;
  until FolderSepI < 0;
end;
*)

function TkvDataset.RecordExists(const AKey: String): Boolean;
var
  HashRecIdx : Word64;
  HashRec : TkvHashFileRecord;
begin
  if AKey = '' then
    raise EkvDiskObject.Create('Invalid key');
  if AKey = '101/10101' then
    HashRecIdx := 1;
  Result := LocateRecord(AKey, HashRecIdx, HashRec);
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

// Constructs a value from a HashRec
function TkvDataset.HashRecToValue(const HashRec: TkvHashFileRecord): AkvValue;
var
  V : AkvValue;
  ValBuf : Pointer;
begin
  Assert(HashRec.RecordType in [hfrtKeyValue, hfrtKeyValueWithHashCollision]);

  case HashRec.ValueType of
    hfrvtNone  :
      raise EkvDiskObject.Create('No value');
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
            FValueFile.ReadChain(HashRec.ValueLongChainIndex, ValBuf^,
                HashRec.ValueSize);
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
    raise EkvDiskObject.Create('Invalid value type');
  end;
end;

procedure TkvDataset.GetAllChildRecords(const BaseIndex: Word64;
          const D: TkvFolderValue);
var
  I : Integer;
  RecI : Word64;
  ChildRec : TkvHashFileRecord;
  Key : String;
  Value : AkvValue;
begin
  for I := 0 to KV_HashFile_LevelSlotCount - 1 do
    begin
      RecI := BaseIndex + Word32(I);
      FHashFile.LoadRecord(RecI, ChildRec);
      case ChildRec.RecordType of
        hfrtEmpty : ;
        hfrtParentSlot :
          GetAllChildRecords(ChildRec.ChildSlotRecordIndex, D);
        hfrtKeyValue :
          begin
            Key := HashRecToKey(ChildRec);
            Value := GetFolderRecord(ChildRec);
            D.Add(Key, Value);
          end;
        hfrtKeyValueWithHashCollision :
          begin
            Key := HashRecToKey(ChildRec);
            Value := GetFolderRecord(ChildRec);
            D.Add(Key, Value);
            GetAllChildRecords(ChildRec.ChildSlotRecordIndex, D);
          end;
      end;
    end;
end;

function TkvDataset.GetAllFolderRecords(const BaseIndex: Word64): AkvValue;
var
  D : TkvFolderValue;
begin
  D := TkvFolderValue.Create;
  try
    GetAllChildRecords(BaseIndex, D);
  except
    D.Free;
    raise;
  end;
  Result := D;
end;

function TkvDataset.GetFolderRecord(const HashRec: TkvHashFileRecord): AkvValue;
begin
  if HashRec.ValueType = hfrvtFolder then
    Result := GetAllFolderRecords(HashRec.ValueFolderBaseIndex)
  else
    Result := HashRecToValue(HashRec);
end;

function TkvDataset.GetAllRecords: AkvValue;
begin
  Result := GetAllFolderRecords(0);
end;

function TkvDataset.GetRecordIfExists(const AKey: String): AkvValue;
var
  HashRecIdx : Word64;
  HashRec : TkvHashFileRecord;
begin
  if AKey = '' then
    raise EkvDiskObject.Create('Invalid key');
  if AKey = '/' then
    Result := GetAllRecords
  else
    if not LocateRecord(AKey, HashRecIdx, HashRec) then
      Result := nil
    else
      Result := GetFolderRecord(HashRec);
end;

function TkvDataset.GetRecord(const AKey: String): AkvValue;
var
  HashRecIdx : Word64;
  HashRec : TkvHashFileRecord;
begin
  if AKey = '' then
    raise EkvDiskObject.Create('Invalid key');
  if AKey = '/' then
    Result := GetAllRecords
  else
    if not LocateRecord(AKey, HashRecIdx, HashRec) then
      raise EkvDiskObject.CreateFmt('Key not found: %s', [AKey])
    else
      Result := GetFolderRecord(HashRec);
end;

function TkvDataset.FolderExists(const AKey: String): Boolean;
var
  HashRecIdx : Word64;
  HashRec : TkvHashFileRecord;
begin
  if AKey = '' then
    raise EkvDiskObject.Create('Invalid key');
  if AKey = '/' then
    Result := True
  else
    if LocateRecord(AKey, HashRecIdx, HashRec) then
      Result := HashRec.ValueType = hfrvtFolder
    else
      Result := False;
end;

procedure TkvDataset.MakePath(const AKeyPath: String);
var
  KeyLen : Integer;
  BaseIdx : Word64;
  StartI : Integer;
  FolderSepI : Integer;
  SubKey : String;
  Timestamp : UInt64;
begin
  KeyLen := Length(AKeyPath);
  if (KeyLen = 0) or (KeyLen > KV_HashFile_MaxKeyLength) then
    raise EkvDiskObject.Create('Invalid key length');

  Timestamp := FHashFile.GetNextTimestamp;
  try
    BaseIdx := 0;
    StartI := 0;
    repeat
      FolderSepI := AKeyPath.IndexOf(Char('/'), StartI);
      if FolderSepI = 0 then
        raise EkvDiskObject.Create('Invalid key: Empty folder name');
      if FolderSepI > 0 then
        begin
          SubKey := Copy(AKeyPath, StartI + 1, FolderSepI - StartI);
          InternalAddRecord(BaseIdx, SubKey, nil, True, BaseIdx, Timestamp);
          StartI := FolderSepI + 1;
        end;
    until FolderSepI < 0;
    if StartI = 0 then
      SubKey := AKeyPath
    else
      if StartI = KeyLen then
        exit
      else
        SubKey := Copy(AKeyPath, StartI + 1, KeyLen - StartI);
    InternalAddRecord(BaseIdx, SubKey, nil, True, BaseIdx, Timestamp);
  finally
    FHashFile.UpdateTimestamp(Timestamp);
    FHashFile.UpdateHeader;
  end;
end;

procedure TkvDataset.AddRecord(const AKey: String; const AValue: AkvValue);
var
  KeyLen : Integer;
  Timestamp : UInt64;
begin
  KeyLen := Length(AKey);
  if (KeyLen = 0) or (KeyLen > KV_HashFile_MaxKeyLength) then
    raise EkvDiskObject.Create('Invalid key length');
  if FUseFolders and AKey.EndsWith('/') then
    raise EkvDiskObject.Create('Invalid key: Folder reference');
  if not Assigned(AValue) then
    raise EkvDiskObject.Create('Invalid value');
  Timestamp := FHashFile.GetNextTimestamp;
  try
    InternalAddKeyValue(AKey, AValue, Timestamp);
    ////UpdateFoldersTimestamp(AKey, Timestamp);
  finally
    FHashFile.UpdateTimestamp(Timestamp);
    FHashFile.UpdateHeader;
  end;
end;

procedure TkvDataset.SetRecord(const AKey: String; const AValue: AkvValue);
var
  HashRecIdx : Word64;
  HashRec : TkvHashFileRecord;
  Timestamp : UInt64;
begin
  if AKey = '' then
    raise EkvDiskObject.Create('Invalid key');
  if not Assigned(AValue) then
    raise EkvDiskObject.Create('Invalid value');
  if not LocateRecord(AKey, HashRecIdx, HashRec) then
    raise EkvDiskObject.CreateFmt('Key not found: %s', [AKey]);
  if HashRec.ValueType = hfrvtFolder then
    raise EkvDiskObject.CreateFmt('Key references a folder: %s', [AKey]);
  Timestamp := FHashFile.GetNextTimestamp;
  HashRecSetValue(HashRec, AValue);
  HashRec.Timestamp := Timestamp;
  try
    FHashFile.SaveRecord(HashRecIdx, HashRec);
  finally
    FHashFile.UpdateTimestamp(Timestamp);
    FHashFile.UpdateHeader;
  end;
end;

procedure TkvDataset.ListOfChildKeys(
          const BaseIdx: Word32;
          const ADict: TkvDictionaryValue;
          const ARecurse: Boolean;
          const AIncludeRecordTimestamp: Boolean);

var
  I : Integer;
  RecI : Word64;
  ChildRec : TkvHashFileRecord;
  Key : String;
  Value : AkvValue;

  function ChildRecToValue: AkvValue;
  begin
    if ChildRec.ValueType = hfrvtFolder then
      if ARecurse then
        Result := ListOfFolderKeys(ChildRec, True, AIncludeRecordTimestamp)
      else
        Result := TkvDictionaryValue.Create
    else
      if AIncludeRecordTimestamp then
        Result := TkvIntegerValue.Create(Int64(ChildRec.Timestamp))
      else
        Result := TkvNullValue.Create;
  end;

begin
  for I := 0 to KV_HashFile_LevelSlotCount - 1 do
    begin
      RecI := BaseIdx + Word32(I);
      FHashFile.LoadRecord(RecI, ChildRec);
      case ChildRec.RecordType of
        hfrtEmpty : ;
        hfrtParentSlot :
          ListOfChildKeys(ChildRec.ChildSlotRecordIndex, ADict, ARecurse,
              AIncludeRecordTimestamp);
        hfrtKeyValue :
          begin
            Key := HashRecToKey(ChildRec);
            Value := ChildRecToValue;
            ADict.Add(Key, Value);
          end;
        hfrtKeyValueWithHashCollision :
          begin
            Key := HashRecToKey(ChildRec);
            Value := ChildRecToValue;
            ADict.Add(Key, Value);
            ListOfChildKeys(ChildRec.ChildSlotRecordIndex, ADict, ARecurse,
                AIncludeRecordTimestamp);
          end;
      end;
    end;
end;

function TkvDataset.ListOfFolderKeys(const HashRec: TkvHashFileRecord;
         const ARecurse: Boolean; const AIncludeRecordTimestamp: Boolean): TkvDictionaryValue;
var
  D : TkvDictionaryValue;
begin
  Assert(HashRec.ValueType = hfrvtFolder);
  D := TkvDictionaryValue.Create;
  try
    ListOfChildKeys(HashRec.ValueFolderBaseIndex, D, ARecurse, AIncludeRecordTimestamp);
  except
    D.Free;
    raise;
  end;
  Result := D;
end;

function TkvDataset.ListOfRootKeys(
         const ARecurse: Boolean;
         const AIncludeRecordTimestamp: Boolean): TkvDictionaryValue;
var
  D : TkvDictionaryValue;
begin
  D := TkvDictionaryValue.Create;
  try
    ListOfChildKeys(0, D, ARecurse, AIncludeRecordTimestamp);
  except
    D.Free;
    raise;
  end;
  Result := D;
end;

procedure TkvDataset.SetOrAddRecord(const AKey: String; const AValue: AkvValue);
var
  KeyLen : Integer;
  HashRecIdx : Word64;
  HashRec : TkvHashFileRecord;
  Timestamp : UInt64;
begin
  KeyLen := Length(AKey);
  if KeyLen = 0 then
    raise EkvDiskObject.Create('Invalid key');
  if not Assigned(AValue) then
    raise EkvDiskObject.Create('Invalid value');
  if not LocateRecord(AKey, HashRecIdx, HashRec) then
    begin
      if KeyLen > KV_HashFile_MaxKeyLength then
        raise EkvDiskObject.Create('Invalid key length');
      if FUseFolders and AKey.EndsWith('/') then
        raise EkvDiskObject.Create('Invalid key: Folder reference');
      Timestamp := FHashFile.GetNextTimestamp;
      try
        InternalAddKeyValue(AKey, AValue, Timestamp);
      finally
        FHashFile.UpdateTimestamp(Timestamp);
        FHashFile.UpdateHeader;
      end;
    end
  else
    begin
      if HashRec.ValueType = hfrvtFolder then
        raise EkvDiskObject.CreateFmt('Key references a folder: %s', [AKey]);
      Timestamp := FHashFile.GetNextTimestamp;
      HashRecSetValue(HashRec, AValue);
      HashRec.Timestamp := Timestamp;
      try
        FHashFile.SaveRecord(HashRecIdx, HashRec);
      finally
        FHashFile.UpdateTimestamp(Timestamp);
        FHashFile.UpdateHeader;
      end;
    end;
end;

procedure TkvDataSet.HashRecAppendValue_Rewrite(var HashRec: TkvHashFileRecord;
          const AValue: AkvValue);
var
  OldVal : AkvValue;
  NewVal : AkvValue;
begin
  OldVal := HashRecToValue(HashRec);
  try
    NewVal := ValueOpAppend(OldVal, AValue);
  finally
    OldVal.Free;
  end;
  try
    HashRecSetValue(HashRec, NewVal);
  finally
    NewVal.Free;
  end;
end;

// Appends value to HashRec value for String or Binary values
procedure TkvDataSet.HashRecAppendValue_StrOrBin(var HashRec: TkvHashFileRecord;
          const AValue: AkvValue);
var
  DataBuf : Pointer;
  DataSize : Integer;
  OldSize : Integer;
  NewSize : Integer;
  NewValDataSize : Word32;
  NewValLength : Word32;
  NewValLenEnc : Word32;
begin
  if AValue.TypeId <> HashRec.ValueTypeId then
    raise EkvDiskObject.Create('Append value type mismatch');

  AValue.GetDataBuf(DataBuf, DataSize);
  OldSize := HashRec.ValueSize;
  NewSize := OldSize + DataSize;

  if NewSize <= KV_HashFileRecord_SlotShortValueSize then
    begin
      HashRec.ValueType := hfrvtShort;
      Move(DataBuf^, HashRec.ValueShort[OldSize], DataSize);
      NewValDataSize := NewSize - 1;
      if HashRec.ValueTypeId = KV_Value_TypeId_String then
        NewValLength := NewValDataSize div 2
      else
        NewValLength := NewValDataSize;
      HashRec.ValueShort[0] := Byte(NewValLength);
      HashRec.ValueSize := NewSize;
    end
  else
  if (NewSize <= 512) or
     (HashRec.ValueType = hfrvtShort) then
    HashRecAppendValue_Rewrite(HashRec, AValue)
  else
    // NewSize needs to be big enough that value's internal VarWord32 length
    // encoding use the 32 bit encoding before AppendChain can be used;
    // and ValueType needs to be hfrvtLong.
    begin
      Assert(HashRec.ValueType = hfrvtLong);
      FValueFile.AppendChain(HashRec.ValueLongChainIndex, DataBuf^, DataSize);
      NewValDataSize := NewSize - SizeOf(Word32);
      if HashRec.ValueTypeId = KV_Value_TypeId_String then
        NewValLength := NewValDataSize div 2
      else
        NewValLength := NewValDataSize;
      kvVarWord32EncodeBuf(NewValLength, NewValLenEnc, SizeOf(Word32));
      FValueFile.WriteChainStart(HashRec.ValueLongChainIndex, NewValLenEnc, SizeOf(Word32));
      HashRec.ValueSize := NewSize;
    end;
end;

procedure TkvDataSet.HashRecAppendValue_List(var HashRec: TkvHashFileRecord;
          const AValue: AkvValue);
var
  List : TkvListValue;
  OldSize : Integer;
  DataSize : Integer;
  DataCount : Integer;
  DataBuf : Pointer;
  I : Integer;
  NewSize : Integer;
  NewCount : Integer;
  NewCountEncSize : Integer;
  NewCountEnc : Word32;
  OldCountEnc : Word32;
  OldCount : Word32;
  OldCountEncSize : Integer;
  F : Integer;
begin
  if not (AValue is TkvListValue) then
    raise EkvDiskObject.Create('Append value type mismatch');
  List := TkvListValue(AValue);
  DataCount := List.GetCount;
  if DataCount = 0 then
    exit;

  DataSize := 0;
  for I := 0 to DataCount - 1 do
    Inc(DataSize, List.GetValue(I).SerialSize + 1);

  OldSize := HashRec.ValueSize;
  NewSize := OldSize + DataSize;

  if (NewSize <= 512) or
     (HashRec.ValueType = hfrvtShort) then
    HashRecAppendValue_Rewrite(HashRec, AValue)
  else
    begin
      Assert(HashRec.ValueType = hfrvtLong);
      FValueFile.ReadChain(HashRec.ValueLongChainIndex, OldCountEnc, SizeOf(Word32));
      OldCountEncSize := kvVarWord32DecodeBuf(OldCountEnc, SizeOf(Word32), OldCount);
      NewCount := Integer(OldCount) + DataCount;
      NewCountEncSize := kvVarWord32EncodedSize(NewCount);
      if NewCountEncSize <> OldCountEncSize then
        HashRecAppendValue_Rewrite(HashRec, AValue)
      else
        begin
          GetMem(DataBuf, DataSize);
          F := List.EncodeEntries(DataBuf^, DataSize);
          Assert(F = DataSize);
          FValueFile.AppendChain(HashRec.ValueLongChainIndex, DataBuf^, DataSize);
          FreeMem(DataBuf);
          kvVarWord32EncodeBuf(NewCount, NewCountEnc, SizeOf(Word32));
          FValueFile.WriteChainStart(HashRec.ValueLongChainIndex, NewCountEnc, NewCountEncSize);
          HashRec.ValueSize := NewSize;
        end;
    end;
end;

procedure TkvDataSet.HashRecAppendValue_Dictionary(var HashRec: TkvHashFileRecord;
          const AValue: AkvValue);
var
  Dict : TkvDictionaryValue;
  OldSize : Integer;
  DataSize : Integer;
  DataCount : Integer;
  DataBuf : Pointer;
  NewSize : Integer;
  NewCount : Integer;
  NewCountEncSize : Integer;
  NewCountEnc : Word32;
  OldCountEnc : Word32;
  OldCount : Word32;
  OldCountEncSize : Integer;
  F : Integer;
begin
  if not (AValue is TkvDictionaryValue) then
    raise EkvDiskObject.Create('Append value type mismatch');
  Dict := TkvDictionaryValue(AValue);
  DataCount := Dict.GetCount;
  if DataCount = 0 then
    exit;

  DataSize := Dict.EncodedEntriesSize;
  OldSize := HashRec.ValueSize;
  NewSize := OldSize + DataSize;

  if (NewSize <= 512) or
     (HashRec.ValueType = hfrvtShort) then
    HashRecAppendValue_Rewrite(HashRec, AValue)
  else
    begin
      Assert(HashRec.ValueType = hfrvtLong);
      FValueFile.ReadChain(HashRec.ValueLongChainIndex, OldCountEnc, SizeOf(Word32));
      OldCountEncSize := kvVarWord32DecodeBuf(OldCountEnc, SizeOf(Word32), OldCount);
      NewCount := Integer(OldCount) + DataCount;
      NewCountEncSize := kvVarWord32EncodedSize(NewCount);
      if NewCountEncSize <> OldCountEncSize then
        HashRecAppendValue_Rewrite(HashRec, AValue)
      else
        begin
          GetMem(DataBuf, DataSize);
          F := Dict.EncodeEntries(DataBuf^, DataSize);
          Assert(F = DataSize);
          FValueFile.AppendChain(HashRec.ValueLongChainIndex, DataBuf^, DataSize);
          FreeMem(DataBuf);
          kvVarWord32EncodeBuf(NewCount, NewCountEnc, SizeOf(Word32));
          FValueFile.WriteChainStart(HashRec.ValueLongChainIndex, NewCountEnc, NewCountEncSize);
          HashRec.ValueSize := NewSize;
        end;
    end;
end;

// Appends value to HashRec value
procedure TkvDataSet.HashRecAppendValue(var HashRec: TkvHashFileRecord;
          const AValue: AkvValue);
begin
  case HashRec.ValueTypeId of
    KV_Value_TypeId_String,
    KV_Value_TypeId_Binary      : HashRecAppendValue_StrOrBin(HashRec, AValue);
    KV_Value_TypeId_List        : HashRecAppendValue_List(HashRec, AValue);
    KV_Value_TypeId_Dictionary,
    KV_Value_TypeId_Folder      : HashRecAppendValue_Dictionary(HashRec, AValue);
  else
    raise EkvDiskObject.Create('Record value type is not appendable');
  end;
end;

procedure TkvDataset.AppendRecord(const AKey: String; const AValue: AkvValue);
var
  HashRecIdx : Word64;
  HashRec : TkvHashFileRecord;
  Timestamp : UInt64;
begin
  if AKey = '' then
    raise EkvDiskObject.Create('Invalid key');
  if not LocateRecord(AKey, HashRecIdx, HashRec) then
    raise EkvDiskObject.CreateFmt('Key not found: %s', [AKey]);
  Assert(HashRec.RecordType in [hfrtKeyValue, hfrtKeyValueWithHashCollision]);
  if HashRec.ValueType = hfrvtFolder then
    raise EkvDiskObject.CreateFmt('Key references a folder: %s', [AKey]);
  Timestamp := FHashFile.GetNextTimestamp;
  HashRecAppendValue(HashRec, AValue);
  HashRec.Timestamp := Timestamp;
  try
    FHashFile.SaveRecord(HashRecIdx, HashRec);
  finally
    FHashFile.UpdateTimestamp(Timestamp);
    FHashFile.UpdateHeader;
  end;
end;

procedure TkvDataset.InternalDeleteKeyValue(const HashRecIdx: Word64;
          var HashRec: TkvHashFileRecord);
begin
  if HashRec.KeyLongChainIndex <> KV_BlobFile_InvalidIndex then
    begin
      FKeyFile.ReleaseChain(HashRec.KeyLongChainIndex);
      HashRec.KeyLongChainIndex := KV_BlobFile_InvalidIndex;
    end;
  if HashRec.ValueType = hfrvtFolder then
    begin
      Assert(HashRec.ValueFolderBaseIndex <> KV_HashFile_InvalidIndex);
      InternalDeleteChildren(HashRec.ValueFolderBaseIndex, False);
      FHashFile.AddDeletedSlots(HashRec.ValueFolderBaseIndex);
      HashRec.ValueFolderBaseIndex := KV_HashFile_InvalidIndex;
    end
  else
    HashRecReleaseValue(HashRec);
end;

procedure TkvDataset.InternalDeleteChildren(const BaseIndex: Word64; const AUpdateRecords: Boolean);
var
  SltI : Word32;
  RecI : Word64;
  ChildHashRec : TkvHashFileRecord;
  ChildRecI : Word64;
  DelRec : Boolean;
begin
  for SltI := 0 to KV_HashFile_LevelSlotCount - 1 do
    begin
      RecI := BaseIndex + SltI;
      FHashFile.LoadRecord(RecI, ChildHashRec);
      DelRec := False;
      case ChildHashRec.RecordType of
        hfrtKeyValue :
          begin
            InternalDeleteKeyValue(RecI, ChildHashRec);
            DelRec := True;
          end;
        hfrtKeyValueWithHashCollision :
          begin
            ChildRecI := ChildHashRec.ChildSlotRecordIndex;
            if ChildRecI <> KV_HashFile_InvalidIndex then
              begin
                InternalDeleteChildren(ChildRecI, False);
                FHashFile.AddDeletedSlots(ChildRecI);
              end;
            InternalDeleteKeyValue(RecI, ChildHashRec);
            DelRec := True;
          end;
        hfrtParentSlot :
          begin
            ChildRecI := ChildHashRec.ChildSlotRecordIndex;
            Assert(ChildRecI <> KV_HashFile_InvalidIndex);
            InternalDeleteChildren(ChildRecI, False);
            FHashFile.AddDeletedSlots(ChildRecI);
            DelRec := True;
          end;
      end;
      if AUpdateRecords and DelRec then
        begin
          ChildHashRec.RecordType := hfrtEmpty;
          FHashFile.SaveRecord(RecI, ChildHashRec);
        end;
    end;
end;

procedure TkvDataset.InternalDeleteRecord(const HashRecIdx: Word64;
          var HashRec: TkvHashFileRecord);
var
  SltI : Word32;
  RecI : Word64;
  ChildRecI : Word64;
  ChildHashRec : TkvHashFileRecord;
  NewHashRec : TkvHashFileRecord;
  R : Boolean;
begin
  InternalDeleteKeyValue(HashRecIdx, HashRec);
  case HashRec.RecordType of
    hfrtKeyValue :
      HashRec.RecordType := hfrtEmpty;
    hfrtKeyValueWithHashCollision :
      begin
        // Replace entry with first non-empty child entry and
        // set that child entry empty
        R := False;
        ChildRecI := HashRec.ChildSlotRecordIndex;
        for SltI := 0 to KV_HashFile_LevelSlotCount - 1 do
          begin
            RecI := ChildRecI + SltI;
            FHashFile.LoadRecord(RecI, ChildHashRec);
            if ChildHashRec.RecordType = hfrtKeyValue then
              begin
                NewHashRec := ChildHashRec;
                NewHashRec.RecordType := hfrtKeyValueWithHashCollision;
                NewHashRec.ChildSlotRecordIndex := ChildRecI;

                ChildHashRec.RecordType := hfrtEmpty;
                FHashFile.SaveRecord(RecI, ChildHashRec);

                HashRec := NewHashRec;
                R := True;
                break;
              end;
          end;
        if not R then // All slots empty
          begin
            FHashFile.AddDeletedSlots(ChildRecI);
            HashRec.ChildSlotRecordIndex := KV_HashFile_InvalidIndex;
            HashRec.RecordType := hfrtEmpty;
          end;
      end;
  end;
  FHashFile.SaveRecord(HashRecIdx, HashRec);
end;

procedure TkvDataset.DeleteRecord(const AKey: String);
var
  HashRecIdx : Word64;
  HashRec : TkvHashFileRecord;
begin
  if AKey = '' then
    raise EkvDiskObject.Create('Invalid key');
  if not LocateRecord(AKey, HashRecIdx, HashRec) then
    raise EkvDiskObject.CreateFmt('Key not found: %s', [AKey]);
  Assert(HashRec.RecordType in [hfrtKeyValue, hfrtKeyValueWithHashCollision]);
  try
    InternalDeleteRecord(HashRecIdx, HashRec);
  finally
    FHashFile.UpdateHeader;
  end;
end;

procedure TkvDataset.DeleteFolderRecords(const APath: String);
var
  HashRecIdx : Word64;
  HashRec : TkvHashFileRecord;
begin
  if APath = '' then
    raise EkvDiskObject.Create('Invalid key');
  if APath = '/' then
    try
      InternalDeleteChildren(0, True);
    finally
      FHashFile.UpdateHeader;
    end
  else
    begin
      if not LocateRecord(APath, HashRecIdx, HashRec) then
        raise EkvDiskObject.CreateFmt('Key not found: %s', [APath]);
      Assert(HashRec.RecordType in [hfrtKeyValue, hfrtKeyValueWithHashCollision]);
      if HashRec.ValueType <> hfrvtFolder then
        raise EkvDiskObject.CreateFmt('Not a folder: %s', [APath]);
      Assert(HashRec.ValueFolderBaseIndex <> KV_HashFile_InvalidIndex);
      try
        InternalDeleteChildren(HashRec.ValueFolderBaseIndex, True);
      finally
        FHashFile.UpdateHeader;
      end;
    end;
end;

function TkvDataset.ListOfKeys(
         const AKeyPath: String;
         const ARecurse: Boolean;
         const AIncludeRecordTimestamp: Boolean): TkvDictionaryValue;
var
  HashRecIdx : Word64;
  HashRec : TkvHashFileRecord;
begin
  if (AKeyPath = '') or (AKeyPath = '/') then
    Result := ListOfRootKeys(ARecurse, AIncludeRecordTimestamp)
  else
    begin
      if not LocateRecord(AKeyPath, HashRecIdx, HashRec) then
        raise EkvDiskObject.CreateFmt('Path not found: %s', [AKeyPath]);
      if HashRec.ValueType <> hfrvtFolder then
        raise EkvDiskObject.CreateFmt('Path does not specify a folder: %s', [AKeyPath]);
      Result := ListOfFolderKeys(HashRec, ARecurse, AIncludeRecordTimestamp);
    end;
end;

function TkvDataset.SetNextIteratorRecord(var AIterator: TkvDatasetIterator): Boolean;
var
  E : PkvDatasetIteratorStackEntry;
  R : Boolean;
  RecIdx : Word64;
begin
  Assert(AIterator.FStackLen > 0);

  E := @AIterator.FStack[AIterator.FStackLen - 1];
  repeat

    R := False;
    repeat
      if E^.SlotIdx = KV_HashFile_LevelSlotCount then
        begin
          Dec(AIterator.FStackLen);
          SetLength(AIterator.FStack, AIterator.FStackLen);
          if AIterator.FStackLen = 0 then
            begin
              Result := False;
              exit;
            end;
          E := @AIterator.FStack[AIterator.FStackLen - 1];
        end
      else
        R := True;
    until R;

    R := False;
    repeat
      Assert(Word32(E^.SlotIdx) < KV_HashFile_LevelSlotCount);
      RecIdx := E^.BaseRecIdx + Word32(E^.SlotIdx);
      FHashFile.LoadRecord(RecIdx, AIterator.FHashRec);
      case AIterator.FHashRec.RecordType of
        hfrtKeyValue :
          case AIterator.FIteratorType of
            ditRecordsNonRecursive,
            ditRecordsRecursive :
              if AIterator.FHashRec.ValueType = hfrvtFolder then
                if AIterator.FIteratorType = ditRecordsRecursive then
                  begin
                    // Recurse down into folder slots
                    Inc(E^.SlotIdx);
                    Inc(AIterator.FStackLen);
                    SetLength(AIterator.FStack, AIterator.FStackLen);
                    E := @AIterator.FStack[AIterator.FStackLen - 1];
                    E^.BaseRecIdx := AIterator.FHashRec.ValueFolderBaseIndex;
                    E^.SlotIdx := 0;
                    E^.FolderName := HashRecToKey(AIterator.FHashRec);
                  end
                else
                  if AIterator.FIncludeFolders then
                    begin
                      // Found folder entry
                      Result := True;
                      exit;
                    end
                  else
                    begin
                      // Skip folder
                      Inc(E^.SlotIdx);
                      if E^.SlotIdx = KV_HashFile_LevelSlotCount then
                        R := True;
                    end
              else
                begin
                  // Found key/value entry
                  // Check Timestamp filter
                  if (AIterator.FMinTimestamp > 0) and (AIterator.FHashRec.Timestamp < AIterator.FMinTimestamp) then
                    begin
                      // Move to next slot
                      Inc(E^.SlotIdx);
                      if E^.SlotIdx = KV_HashFile_LevelSlotCount then
                        R := True;
                    end
                  else
                    begin
                      Result := True;
                      exit;
                    end;
                end;
            ditFoldersNonRecursive :
              if AIterator.FHashRec.ValueType = hfrvtFolder then
                begin
                  // Found folder
                  Result := True;
                  exit;
                end
              else
                begin
                  // Not a folder, move on to next slot
                  Inc(E^.SlotIdx);
                  if E^.SlotIdx = KV_HashFile_LevelSlotCount then
                    R := True;
                end;
          else
            raise EkvDiskObject.Create('Invalid iterator type');
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
            Inc(AIterator.FStackLen);
            SetLength(AIterator.FStack, AIterator.FStackLen);
            E := @AIterator.FStack[AIterator.FStackLen - 1];
            E^.BaseRecIdx := AIterator.FHashRec.ChildSlotRecordIndex;
            E^.SlotIdx := 0;
          end;
        hfrtKeyValueWithHashCollision :
          begin
            // Recurse down and use current key/value entry
            Inc(E^.SlotIdx);
            Inc(AIterator.FStackLen);
            SetLength(AIterator.FStack, AIterator.FStackLen);
            E := @AIterator.FStack[AIterator.FStackLen - 1];
            E^.BaseRecIdx := AIterator.FHashRec.ChildSlotRecordIndex;
            E^.SlotIdx := -1;
            Result := True;
            exit;
          end;
      else
        raise EkvDiskObject.Create('Bad hash record type');
      end;
    until R;
  until false;
end;

function TkvDataset.IterateFirst(const AIteratorType: TkvDatasetIteratorType;
         const APath: String;
         out AIterator: AkvDatasetIterator;
         const AIncludeFolders: Boolean;
         const AMinTimestamp: UInt64): Boolean;
var
  It : TkvDatasetIterator;
  E : PkvDatasetIteratorStackEntry;
  BaseRecIdx : Word64;
  HashRecIdx : Word64;
  HashRec : TkvHashFileRecord;
begin
  if APath = '' then
    BaseRecIdx := 0
  else
    begin
      if not LocateRecord(APath, HashRecIdx, HashRec) then
        raise EkvDiskObject.CreateFmt('Path not found: %s', [APath]);
      if HashRec.ValueType <> hfrvtFolder then
        raise EkvDiskObject.CreateFmt('Path not a folder: %s', [APath]);
      BaseRecIdx := HashRec.ValueFolderBaseIndex;
    end;
  It := TkvDatasetIterator.Create;
  try
    It.FIteratorType := AIteratorType;
    It.FIncludeFolders := AIncludeFolders;
    It.FDatabaseName := FDatabaseName;
    It.FDatasetName := FName;
    It.FPath := APath;
    It.FDataset := self;
    It.FMinTimestamp := AMinTimestamp;
    It.FStackLen := 1;
    SetLength(It.FStack, 1);
    E := @It.FStack[0];
    E^.BaseRecIdx := BaseRecIdx;
    E^.SlotIdx := 0;
    Result := SetNextIteratorRecord(It);
  except
    FreeAndNil(It);
    raise;
  end;
  if not Result then
    FreeAndNil(It);
  AIterator := It;
end;

function TkvDataset.IterateRecords(const APath: String;
         out AIterator: AkvDatasetIterator;
         const ARecurse: Boolean;
         const AIncludeFolders: Boolean;
         const AMinTimestamp: UInt64): Boolean;
var
  ItTy : TkvDatasetIteratorType;
begin
  if ARecurse then
    ItTy := ditRecordsRecursive
  else
    ItTy := ditRecordsNonRecursive;
  Result := IterateFirst(ItTy, APath, AIterator, AIncludeFolders, AMinTimestamp);
end;

function TkvDataset.IterateFolders(const APath: String; out AIterator: AkvDatasetIterator): Boolean;
begin
  Result := IterateFirst(ditFoldersNonRecursive, APath, AIterator, True, 0);
end;

function TkvDataset.IterateNextRecord(var AIterator: AkvDatasetIterator): Boolean;
var
  It : TkvDatasetIterator;
  StackLen : Integer;
  E : PkvDatasetIteratorStackEntry;
begin
  It := AIterator as TkvDatasetIterator;
  Assert(It.FDataset = self);

  StackLen := It.FStackLen;
  if StackLen = 0 then
    raise EkvDiskObject.Create('Next past end');
  E := @It.FStack[StackLen - 1];
  Inc(E^.SlotIdx);

  Result := SetNextIteratorRecord(It);
end;

function TkvDataset.IteratorHasRecord(const AIterator: AkvDatasetIterator): Boolean;
var
  It : TkvDatasetIterator;
begin
  It := AIterator as TkvDatasetIterator;
  Assert(It.FDataset = self);
  Result := It.FStackLen > 0;
end;

function TkvDataset.IteratorIsFolder(const AIterator: AkvDatasetIterator): Boolean;
var
  It : TkvDatasetIterator;
begin
  It := AIterator as TkvDatasetIterator;
  Assert(It.FDataset = self);
  if not IteratorHasRecord(AIterator) then
    raise EkvDiskObject.Create('No record');
  Result := It.FHashRec.ValueType = hfrvtFolder;
end;

function TkvDataset.IteratorGetKey(const AIterator: AkvDatasetIterator): String;
var
  It : TkvDatasetIterator;
  S, F : String;
  I : Integer;
begin
  It := AIterator as TkvDatasetIterator;
  Assert(It.FDataset = self);
  if not IteratorHasRecord(AIterator) then
    raise EkvDiskObject.Create('No record');
  S := HashRecToKey(It.FHashRec);
  for I := It.FStackLen - 1 downto 0 do
    begin
      F := It.FStack[I].FolderName;
      if F <> '' then
        S := F + '/' + S;
    end;
  F := It.FPath;
  if F <> '' then
    S := F + '/' + S;
  Result := S;
end;

function TkvDataset.IteratorGetValue(const AIterator: AkvDatasetIterator): AkvValue;
var
  It : TkvDatasetIterator;
begin
  It := AIterator as TkvDatasetIterator;
  Assert(It.FDataset = self);
  if not IteratorHasRecord(AIterator) then
    raise EkvDiskObject.Create('No record');
  Result := HashRecToValue(It.FHashRec);
end;

function TkvDataset.IteratorGetTimestamp(const AIterator: AkvDatasetIterator): UInt64;
var
  It : TkvDatasetIterator;
begin
  It := AIterator as TkvDatasetIterator;
  Assert(It.FDataset = self);
  if not IteratorHasRecord(AIterator) then
    raise EkvDiskObject.Create('No record');
  Result := It.FHashRec.Timestamp;
end;



{ TkvDatasetListIterator }

function TkvDatasetListIterator.GetDatabase: AkvDatabase;
begin
  Result := FDatabase;
end;

function TkvDatasetListIterator.GetName: String;
begin
  Result := FKey;
end;

function TkvDatasetListIterator.GetDataset: AkvDataset;
begin
  Result := FDataset;
end;



{ TkvDatasetList }

constructor TkvDatasetList.Create(const APath, ASystemName, ADatabaseName: String);
begin
  Assert(ASystemName <> '');
  Assert(ADatabaseName <> '');

  inherited Create;
  FPath := APath;
  FSystemName := ASystemName;
  FDatabaseName := ADatabaseName;

  FList := TkvStringHashList.Create(False, False, True);
  FFile := TkvDatasetListFile.Create(APath, ASystemName, ADatabaseName);
end;

destructor TkvDatasetList.Destroy;
begin
  FreeAndNil(FFile);
  FreeAndNil(FList);
  inherited Destroy;
end;

procedure TkvDatasetList.Finalise;
begin
  if Assigned(FFile) then
    FFile.Finalise;
  if Assigned(FList) then
    FList.Finalise;
end;

procedure TkvDatasetList.ListClear;
begin
  FList.Clear;
end;

procedure TkvDatasetList.ListAppend(const AItem: TkvDataset);
begin
  Assert(Assigned(AItem));
  FList.Add(AItem.Name, AItem);
end;

procedure TkvDatasetList.OpenNew;
begin
  ListClear;
  FFile.OpenNew;
end;

procedure TkvDatasetList.Open(const AHashFileCacheEntries: Word32);
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
          if (Rec.KeyBlobRecordSize = 0) or (Rec.ValBlobRecordSize = 0) then
            begin
              //// temporary: upgrade record (0.17 -> 0.18)
              Rec.KeyBlobRecordSize := KV_Dataset_KeyBlob_DefaultRecordSize;
              Rec.ValBlobRecordSize := KV_Dataset_ValBlob_DefaultRecordSize;
              FFile.SaveRecord(RecIdx, Rec);
            end;
          Dataset := TkvDataset.Create(FPath, FSystemName, FDatabaseName, RecIdx, Rec, AHashFileCacheEntries);
          ListAppend(Dataset);
          Dataset.Open;
        end;
    end;
end;

procedure TkvDatasetList.Close;
var
  It : AkvDatasetListIterator;
begin
  if IterateFirst(It) then
    try
      repeat
        (It as TkvDatasetListIterator).FDataset.Close;
      until not IterateNext(It);
    finally
      FreeAndNil(It);
    end;
  FFile.Close;
end;

procedure TkvDatasetList.Delete;
var
  It : AkvDatasetListIterator;
begin
  if IterateFirst(It) then
    try
      repeat
        (It as TkvDatasetListIterator).FDataset.Delete;
      until not IterateNext(It);
    finally
      FreeAndNil(It);
    end;
  FFile.Delete;
end;

function TkvDatasetList.GetCount: Integer;
begin
  Result := FList.Count;
end;

function TkvDatasetList.IterateFirst(var AIterator: AkvDatasetListIterator): Boolean;
var
  It : TkvStringHashListIterator;
  Itm : PkvStringHashListItem;
  LIt : TkvDatasetListIterator;
begin
  Itm := FList.IterateFirst(It);
  if not Assigned(Itm) then
    begin
      AIterator := nil;
      Result := False;
      exit;
    end;
  LIt := TkvDatasetListIterator.Create;
  LIt.FIterator := It;
  LIt.FItem := Itm;
  LIt.FKey := LIt.FItem^.Key;
  LIt.FDataset := TkvDataset(LIt.FItem.Value);
  AIterator := LIt;
  Result := True;
end;

function TkvDatasetList.IterateNext(var AIterator: AkvDatasetListIterator): Boolean;
var
  LIt : TkvDatasetListIterator;
  R : Boolean;
begin
  LIt := AIterator as TkvDatasetListIterator;
  LIt.FItem := FList.IterateNext(LIt.FIterator);
  R := Assigned(LIt.FItem);
  if R then
    begin
      LIt.FKey := LIt.FItem.Key;
      LIt.FDataset := TkvDataset(LIt.FItem.Value);
    end;
  Result := R;
end;

function TkvDatasetList.Exists(const AName: String): Boolean;
begin
  Result := FList.KeyExists(AName);
end;

function TkvDatasetList.Add(const AName: String;
         const AUseFolders: Boolean;
         const AKeyBlobRecordSize: Word32;
         const AValBlobRecordSize: Word32;
         const AHashFileCacheEntries: Word32): TkvDataset;
var
  Rec : TkvDatasetListFileRecord;
  RecIdx : Word32;
  Dataset : TkvDataset;
begin
  kvInitDatasetListFileRecord(Rec, AName, AUseFolders,
      AKeyBlobRecordSize, AValBlobRecordSize);
  RecIdx := FFile.AppendRecord(Rec);
  Dataset := TkvDataset.Create(FPath, FSystemName, FDatabaseName, RecIdx, Rec, AHashFileCacheEntries);
  ListAppend(Dataset);
  Result := Dataset;
end;

function TkvDatasetList.RequireItemByName(const AName: String): TkvDataset;
begin
  Result := TkvDataset(FList.RequireValue(AName));
end;

procedure TkvDatasetList.Remove(const AName: String);
var
  DsO : TObject;
  Ds : TkvDataset;
begin
  Assert(Assigned(FFile));

  if not FList.GetValue(AName, DsO) then
    raise EkvDiskObject.CreateFmt('Dataset not found: %s', [AName]);
  Ds := TkvDataset(DsO);
  Include(Ds.FDatasetListRec.Flags, dslfrfDeleted);
  FFile.SaveRecord(Ds.FDatasetListIdx, Ds.FDatasetListRec);
  Ds.Finalise;
  FList.DeleteKey(AName);
end;

procedure TkvDatasetList.SaveDataset(const ADataset: TkvDataset);
begin
  Assert(Assigned(ADataset));
  Assert(Assigned(FFile));

  FFile.SaveRecord(ADataset.FDatasetListIdx, ADataset.FDatasetListRec);
end;



{ TkvDatabase }

constructor TkvDatabase.Create(
            const ASystem: TkvSystem;
            const APath, ASystemName: String;
            const ADatabaseListIdx: Word32;
            const ADatabaseListRec: TkvDatabaseListFileRecord);
begin
  Assert(ASystemName <> '');

  inherited Create;

  FSystem := ASystem;
  FPath := APath;
  FDatabaseListIdx := ADatabaseListIdx;
  FDatabaseListRec := ADatabaseListRec;

  kvNameFromBuf(FName, FDatabaseListRec.Name[0], FDatabaseListRec.NameLength);

  FDatasetList := TkvDatasetList.Create(APath, ASystemName, FName);
end;

destructor TkvDatabase.Destroy;
begin
  FreeAndNil(FDatasetList);
  inherited Destroy;
end;

procedure TkvDatabase.Finalise;
begin
  if Assigned(FDatasetList) then
    FDatasetList.Finalise;
  FSystem := nil;
end;

function TkvDatabase.GetName: String;
begin
  Result := FName;
end;

procedure TkvDatabase.OpenNew;
begin
  FDatasetList.OpenNew;
end;

procedure TkvDatabase.Open(const AHashFileCacheEntries: Word32);
begin
  FDatasetList.Open(AHashFileCacheEntries);
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
  FSystem.FDatabaseList.SaveDatabase(self);
  Result := C;
end;

function TkvDatabase.GetDatasetCount: Integer;
begin
  Result := FDatasetList.GetCount;
end;

function TkvDatabase.IterateFirstDataset(var AIterator: AkvDatasetListIterator): Boolean;
begin
  Result := FDatasetList.IterateFirst(AIterator);
  if Assigned(AIterator) then
    (AIterator as TkvDatasetListIterator).FDatabase := self;
end;

function TkvDatabase.IterateNextDataset(var AIterator: AkvDatasetListIterator): Boolean;
begin
  Result := FDatasetList.IterateNext(AIterator);
end;

function TkvDatabase.RequireDatasetByName(const AName: String): AkvDataset;
begin
  if AName = '' then
    raise EkvDiskObject.Create('Dataset name required');

  Result := FDatasetList.RequireItemByName(AName);
end;

function TkvDatabase.DatasetExists(const AName: String): Boolean;
begin
  Result := FDatasetList.Exists(AName);
end;

function TkvDatabase.AddDiskDataset(const AName: String;
         const AUseFolders: Boolean;
         const AKeyBlobRecordSize: Word32;
         const AValBlobRecordSize: Word32;
         const AHashFileCacheEntries: Word32): TkvDataset;
var
  Dataset : TkvDataSet;
begin
  Dataset := FDatasetList.Add(
      AName, AUseFolders,
      AKeyBlobRecordSize, AValBlobRecordSize,
      AHashFileCacheEntries);
  Dataset.OpenNew;
  Result := Dataset;
end;

function TkvDatabase.AddDataset(const AName: String; const AUseFolders: Boolean): AkvDataset;
begin
  Result := AddDiskDataset(AName, AUseFolders);
end;

procedure TkvDatabase.RemoveDataset(const AName: String);
var
  Dataset : TkvDataSet;
begin
  Dataset := FDatasetList.RequireItemByName(AName);
  Dataset.Close;
  Dataset.Delete;
  FDatasetList.Remove(AName);
end;



{ TkvDatabaseListIterator }

function TkvDatabaseListIterator.GetName: String;
begin
  Result := FKey;
end;

function TkvDatabaseListIterator.GetDatabase: AkvDatabase;
begin
  Result := FDatabase;
end;



{ TkvDatabaseList }

constructor TkvDatabaseList.Create(const ASystem: TkvSystem; const APath, ASystemName: String);
begin
  Assert(ASystemName <> '');

  inherited Create;
  FSystem := ASystem;
  FPath := APath;
  FSystemName := ASystemName;

  FList := TkvStringHashList.Create(False, False, True);
  FFile := TkvDatabaseListFile.Create(APath, ASystemName);
end;

destructor TkvDatabaseList.Destroy;
begin
  FreeAndNil(FFile);
  FreeAndNil(FList);
  inherited Destroy;
end;

procedure TkvDatabaseList.Finalise;
begin
  if Assigned(FFile) then
    FFile.Finalise;
  if Assigned(FList) then
    FList.Finalise;
  FSystem := nil;
end;

procedure TkvDatabaseList.ListClear;
begin
  FList.Clear;
end;

procedure TkvDatabaseList.ListAppend(const AItem: TkvDatabase);
begin
  Assert(Assigned(AItem));
  FList.Add(AItem.Name, AItem);
end;

procedure TkvDatabaseList.OpenNew;
begin
  ListClear;
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
          Database := TkvDatabase.Create(FSystem, FPath, FSystemName, RecIdx, Rec);
          ListAppend(Database);
          Database.Open;
        end;
    end;
end;

procedure TkvDatabaseList.Close;
var
  It : AkvDatabaseListIterator;
begin
  if IterateFirst(It) then
    try
      repeat
        (It as TkvDatabaseListIterator).FDatabase.Close;
      until not IterateNext(It);
    finally
      FreeAndNil(It);
    end;
  FFile.Close;
end;

procedure TkvDatabaseList.Delete;
var
  It : AkvDatabaseListIterator;
begin
  if IterateFirst(It) then
    try
      repeat
        (It as TkvDatabaseListIterator).FDatabase.Delete;
      until not IterateNext(It);
    finally
      FreeAndNil(It);
    end;
  FFile.Delete;
end;

function TkvDatabaseList.GetCount: Integer;
begin
  Result := FList.Count;
end;

function TkvDatabaseList.IterateFirst(var AIterator: AkvDatabaseListIterator): Boolean;
var
  It : TkvStringHashListIterator;
  Itm : PkvStringHashListItem;
  LIt : TkvDatabaseListIterator;
begin
  Itm := FList.IterateFirst(It);
  if not Assigned(Itm) then
    begin
      AIterator := nil;
      Result := False;
      exit;
    end;
  LIt := TkvDatabaseListIterator.Create;
  LIt.FIterator := It;
  LIt.FItem := Itm;
  LIt.FKey := LIt.FItem^.Key;
  LIt.FDatabase := TkvDatabase(LIt.FItem.Value);
  AIterator := LIt;
  Result := True;
end;

function TkvDatabaseList.IterateNext(var AIterator: AkvDatabaseListIterator): Boolean;
var
  LIt : TkvDatabaseListIterator;
  R : Boolean;
begin
  LIt := AIterator as TkvDatabaseListIterator;
  LIt.FItem := FList.IterateNext(LIt.FIterator);
  R := Assigned(LIt.FItem);
  if R then
    begin
      LIt.FKey := LIt.FItem.Key;
      LIt.FDatabase := TkvDatabase(LIt.FItem.Value);
    end;
  Result := R;
end;

function TkvDatabaseList.DatabaseExists(const AName: String): Boolean;
begin
  Result := FList.KeyExists(AName);
end;

function TkvDatabaseList.AddDatabase(const AName: String): TkvDatabase;
var
  Rec : TkvDatabaseListFileRecord;
  RecIdx : Word32;
  Database : TkvDatabase;
begin
  Assert(AName <> '');

  kvInitDatabaseListFileRecord(Rec, AName);
  RecIdx := FFile.AppendRecord(Rec);
  Database := TkvDatabase.Create(FSystem, FPath, FSystemName, RecIdx, Rec);
  Database.OpenNew;
  ListAppend(Database);
  Result := Database;
end;

function TkvDatabaseList.GetDatabaseByName(const AName: String): TkvDatabase;
var
  V : TObject;
begin
  if not FList.GetValue(AName, V) then
    Result := nil
  else
    Result := TkvDatabase(V);
end;

function TkvDatabaseList.RequireDatabaseByName(const AName: String): TkvDatabase;
begin
  Result := TkvDatabase(FList.RequireValue(AName));
end;

procedure TkvDatabaseList.SaveDatabase(const ADatabase: TkvDatabase);
begin
  Assert(Assigned(ADatabase));
  Assert(Assigned(FFile));

  FFile.SaveRecord(ADatabase.FDatabaseListIdx, ADatabase.FDatabaseListRec);
end;

procedure TkvDatabaseList.Remove(const AName: String);
var
  DbO : TObject;
  Db : TkvDatabase;
begin
  Assert(Assigned(FFile));

  if not FList.GetValue(AName, DbO) then
    raise EkvDiskObject.CreateFmt('Database not found: %s', [AName]);
  Db := TkvDatabase(DbO);
  Include(Db.FDatabaseListRec.Flags, dblfrfDeleted);
  FFile.SaveRecord(Db.FDatabaseListIdx, Db.FDatabaseListRec);
  Db.Finalise;
  FList.DeleteKey(AName);
end;



{ TkvSystem }

constructor TkvSystem.Create(const APath, AName: String);
begin
  if AName = '' then
    raise EkvDiskObject.Create('System name required');

  inherited Create;

  FPath := APath;
  FName := AName;

  FOpen := False;
  FSystemFile := TkvSystemFile.Create(APath, AName);
  FDatabaseList := TkvDatabaseList.Create(self, APath, AName);
end;

destructor TkvSystem.Destroy;
begin
  FreeAndNil(FDatabaseList);
  FreeAndNil(FSystemFile);
  inherited Destroy;
end;

procedure TkvSystem.Finalise;
begin
  if Assigned(FDatabaseList) then
    FDatabaseList.Finalise;
  if Assigned(FSystemFile) then
    FSystemFile.Finalise;
end;

procedure TkvSystem.VerifyNotOpen;
begin
  if FOpen then
    raise EkvDiskObject.Create('Operation not allowed while system is open');
end;

procedure TkvSystem.VerifyOpen;
begin
  if not FOpen then
    raise EkvDiskObject.Create('Operation not allowed while system is closed');
end;

function TkvSystem.Exists: Boolean;
begin
  Result := FSystemFile.Exists;
end;

procedure TkvSystem.OpenNew(const AUserDataStr: String);
begin
  VerifyNotOpen;
  if FPath <> '' then
    ForceDirectories(FPath);
  FSystemFile.OpenNew(AUserDataStr);
  FDatabaseList.OpenNew;
  FOpen := True;
end;

procedure TkvSystem.Open;
begin
  VerifyNotOpen;
  FSystemFile.Open;
  FDatabaseList.Open;
  FOpen := True;
end;

procedure TkvSystem.Close;
begin
  VerifyOpen;
  FDatabaseList.Close;
  FSystemFile.Close;
  FOpen := False;
end;

procedure TkvSystem.Delete;
begin
  VerifyNotOpen;
  FDatabaseList.Delete;
  FSystemFile.Delete;
end;

function TkvSystem.GetUserDataStr: String;
begin
  VerifyOpen;
  Result := FSystemFile.UserDataStr;
end;

procedure TkvSystem.SetUserDataStr(const AUserDataStr: String);
begin
  VerifyOpen;
  FSystemFile.UserDataStr := AUserDataStr;
end;

function TkvSystem.AllocateUniqueId: UInt64;
begin
  VerifyOpen;
  Result := FSystemFile.AllocateUniqueId;
end;

function TkvSystem.DatabaseExists(const AName: String): Boolean;
begin
  Result := FDatabaseList.DatabaseExists(AName);
end;

function TkvSystem.CreateDatabase(const AName: String): AkvDatabase;
begin
  VerifyOpen;
  if AName = '' then
    raise EkvDiskObject.Create('Database name required');

  if DatabaseExists(AName) then
    raise EkvDiskObject.CreateFmt('Database exists: %s', [AName]);

  Result := FDatabaseList.AddDatabase(AName);
end;

function TkvSystem.RequireDatabaseByName(const AName: String): AkvDatabase;
begin
  VerifyOpen;
  if AName = '' then
    raise EkvDiskObject.Create('Database name required');
  Result := FDatabaseList.RequireDatabaseByName(AName);
end;

procedure TkvSystem.DropDatabase(const AName: String);
var
  Db : TkvDatabase;
begin
  Db := RequireDatabaseByName(AName) as TkvDatabase;
  Db.Close;
  Db.Delete;
  FDatabaseList.Remove(AName);
end;

function TkvSystem.GetDatabaseCount: Integer;
begin
  Result := FDatabaseList.GetCount;
end;

function TkvSystem.IterateFirstDatabase(var AIterator: AkvDatabaseListIterator): Boolean;
begin
  Result := FDatabaseList.IterateFirst(AIterator);
end;

function TkvSystem.IterateNextDatabase(var AIterator: AkvDatabaseListIterator): Boolean;
begin
  Result := FDatabaseList.IterateNext(AIterator);
end;

function TkvSystem.CreateDiskDataset(const ADatabaseName, ADatasetName: String;
         const AUseFolders: Boolean;
         const AKeyBlobRecordSize: Word32;
         const AValBlobRecordSize: Word32;
         const AHashFileCacheEntries: Word32): TkvDataset;
var
  Db : TkvDatabase;
  Ds : TkvDataset;
begin
  if ADatasetName = '' then
    raise EkvDiskObject.Create('Dataset name required');

  Db := RequireDatabaseByName(ADatabaseName) as TkvDatabase;
  if Db.DatasetExists(ADatasetName) then
    raise EkvDiskObject.CreateFmt('Dataset exists: %s:%s', [ADatabaseName, ADatasetName]);

  Ds := Db.AddDiskDataset(
      ADatasetName, AUseFolders,
      AKeyBlobRecordSize, AValBlobRecordSize,
      AHashFileCacheEntries);
  Result := Ds;
end;

function TkvSystem.Backup(
         const ABackupPath: String;
         const AHashFileCacheEntries: Word32): TkvSystem;
var
  BakSys : TkvSystem;
  SysHdr : PkvSystemFileHeader;
  BakSysHdr : PkvSystemFileHeader;
  DbIt : AkvDatabaseListIterator;
  Db : TkvDatabase;
  BakDb : TkvDatabase;
  DsIt : AkvDatasetListIterator;
  Ds : TkvDataset;
  BakDs : TkvDataset;
begin
  VerifyOpen;
  BakSys := TkvSystem.Create(ABackupPath, FName);
  try
    BakSys.OpenNew(GetUserDataStr);
    SysHdr := FSystemFile.GetHeader;
    BakSysHdr := BakSys.FSystemFile.GetHeader;
    BakSysHdr^.UniqueIdCounter := SysHdr^.UniqueIdCounter;
    BakSys.FSystemFile.HeaderModified;
    if FDatabaseList.IterateFirst(DbIt) then
      try
        repeat
          Db := DbIt.GetDatabase as TkvDatabase;
          BakDb := BakSys.CreateDatabase(Db.Name) as TkvDatabase;
          BakDb.FDatabaseListRec.UniqueIdCounter := Db.FDatabaseListRec.UniqueIdCounter;
          BakSys.FDatabaseList.SaveDatabase(BakDb);
          if Db.IterateFirstDataset(DsIt) then
            repeat
              Ds := (DsIt as TkvDatasetListIterator).FDataset;
              BakDs := BakSys.CreateDiskDataset(
                  BakDb.Name,
                  Ds.Name,
                  Ds.UseFolders,
                  Ds.KeyBlobRecordSize,
                  Ds.ValBlobRecordSize,
                  AHashFileCacheEntries);
              Ds.BackupTo(BakDs);
            until not Db.IterateNextDataset(DsIt);
        until not FDatabaseList.IterateNext(DbIt);
      finally
        FreeAndNil(DbIt);
      end;
  except
    BakSys.Finalise;
    FreeAndNil(BakSys);
    raise;
  end;
  Result := BakSys;
end;



end.

