{ KeyVast - A key value store }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/02/08  0.01  Initial version }
{ 2018/03/02  0.02  Remove LongWord references }

{$INCLUDE kvInclude.inc}

unit kvHashList;

interface

uses
  SysUtils;



type
  Word32 = FixedUInt;



{ TkvStringHashList }

type
  EkvStringHashList = class(Exception);

  TkvStringHashListItem = record
    Key   : String;
    Value : TObject;
  end;
  PkvStringHashListItem = ^TkvStringHashListItem;

  TkvStringHashListSlot = record
    Count : Integer;
    List  : array of TkvStringHashListItem;
  end;
  PkvStringHashListSlot = ^TkvStringHashListSlot;
  TkvStringHashListSlotArray = array of TkvStringHashListSlot;

  TkvStringHashListIterator = record
    SlotIdx : Integer;
    ItemIdx : Integer;
  end;

  TkvStringHashList = class
  private
    FCaseSensitive   : Boolean;
    FAllowDuplicates : Boolean;
    FItemOwner       : Boolean;

    FCount : Integer;
    FSlots : Integer;
    FList  : TkvStringHashListSlotArray;

    procedure AddToSlot(const SlotIdx: Integer; const Key: String; const Value: TObject);
    procedure ExpandSlots(const NewSlotCount: Integer);
    function  LocateItemIndexBySlot(const SlotIdx: Integer; const Key: String;
              var Item: PkvStringHashListItem): Integer;
    function  LocateItemBySlot(const SlotIdx: Integer; const Key: String;
              var Item: PkvStringHashListItem): Boolean;
    function  LocateItem(const Key: String;
              var Item: PkvStringHashListItem): Boolean;
    function  RequireItem(const Key: String): PkvStringHashListItem;
    function  IterateGetNext(var Iterator: TkvStringHashListIterator): PkvStringHashListItem;

  public
    constructor Create(const CaseSensitive, AllowDuplicates, ItemOwner: Boolean);
    destructor Destroy; override;

    property  Count: Integer read FCount;
    procedure Add(const Key: String; const Value: TObject);
    function  KeyExists(const Key: String): Boolean;
    function  GetValue(const Key: String; var Value: TObject): Boolean;
    function  RequireValue(const Key: String): TObject;
    procedure SetValue(const Key: String; const Value: TObject);
    procedure DeleteKey(const Key: String);
    procedure Clear;
    function  IterateFirst(var Iterator: TkvStringHashListIterator): PkvStringHashListItem;
    function  IterateNext(var Iterator: TkvStringHashListIterator): PkvStringHashListItem;
  end;



{ Utilities }

function kvhlHashString(const S: String; const CaseSensitive: Boolean = True): Word32;



implementation



{ Helpers }

function kvhlHashString(const S: String; const CaseSensitive: Boolean = True): Word32;
var
  H : Word32;
  I : Integer;
  C : WideChar;
  F : Word32;
begin
  H := $5A1F7304;
  for I := 1 to Length(S) do
    begin
      C := S[I];
      F := Ord(C);
      if not CaseSensitive then
        if (F >= Ord('a')) and (F <= Ord('z')) then
          F := F - 32;
      F := F xor Word32(F shl 7) xor Word32(F shl 14) xor Word32(F shl 21) xor Word32(F shl 28);
      H := H xor F;
      H := Word32(UInt64(H shl 5) + (H shr 5));
    end;
  Result := H;
end;

function SameKey(const S1, S2: String; const CaseSensitive: Boolean): Boolean;
begin
  if CaseSensitive then
    Result := (S1 = S2)
  else
    Result := SameText(S1, S2);
end;



{ TkvStringHashList }

const
  kvStringHashList_InitialSlots = 16;
  kvStringHashList_InitialItemsPerSlot = 8;
  kvStringHashList_TargetItemsPerSlot = 4;
  kvStringHashList_SlotExpandFactor = 16;

constructor TkvStringHashList.Create(const CaseSensitive, AllowDuplicates, ItemOwner: Boolean);
var
  I : Integer;
begin
  inherited Create;
  FCaseSensitive := CaseSensitive;
  FAllowDuplicates := AllowDuplicates;
  FItemOwner := ItemOwner;
  FSlots := kvStringHashList_InitialSlots;
  SetLength(FList, FSlots);
  for I := 0 to FSlots - 1 do
    FList[I].Count := 0;
end;

destructor TkvStringHashList.Destroy;
var
  I, J : Integer;
  Slt : PkvStringHashListSlot;
  Itm : PkvStringHashListItem;
begin
  if FItemOwner then
    for I := Length(FList) - 1 downto 0 do
      begin
        Slt := @FList[I];
        for J := Slt^.Count - 1 downto 0 do
          begin
            Itm := @Slt^.List[J];
            Itm^.Value.Free;
          end;
      end;
  inherited Destroy;
end;

procedure TkvStringHashList.AddToSlot(const SlotIdx: Integer;
          const Key: String; const Value: TObject);
var
  Slt : PkvStringHashListSlot;
  Cnt : Integer;
  ICn : Integer;
  Itm : PkvStringHashListItem;
begin
  Assert(SlotIdx >= 0);
  Assert(SlotIdx < FSlots);

  Slt := @FList[SlotIdx];
  Cnt := Length(Slt^.List);
  ICn := Slt^.Count;
  if Cnt = 0 then
    begin
      Cnt := kvStringHashList_InitialItemsPerSlot;
      SetLength(Slt^.List, Cnt);
    end
  else
  if ICn = Cnt then
    begin
      Cnt := Cnt * 2;
      SetLength(Slt^.List, Cnt);
    end;

  Itm := @Slt^.List[ICn];

  Itm^.Key := Key;
  Itm^.Value := Value;

  Slt^.Count := ICn + 1;
end;

procedure TkvStringHashList.ExpandSlots(const NewSlotCount: Integer);
var
  OldList : TkvStringHashListSlotArray;
  NewList : TkvStringHashListSlotArray;
  I, J : Integer;
  Slt : PkvStringHashListSlot;
  Itm : PkvStringHashListItem;
  Hsh : Word32;
  SltI : Integer;
begin
  OldList := FList;
  SetLength(NewList, NewSlotCount);
  for I := 0 to NewSlotCount - 1 do
    NewList[I].Count := 0;
  FList := NewList;
  FSlots := NewSlotCount;
  for I := 0 to Length(OldList) - 1 do
    begin
      Slt := @OldList[I];
      for J := 0 to Slt^.Count - 1 do
        begin
          Itm := @Slt^.List[J];
          Hsh := kvhlHashString(Itm^.Key, FCaseSensitive);
          SltI := Hsh mod Word32(NewSlotCount);
          AddToSlot(SltI, Itm^.Key, Itm^.Value);
        end;
    end;
end;

function TkvStringHashList.LocateItemIndexBySlot(const SlotIdx: Integer; const Key: String;
         var Item: PkvStringHashListItem): Integer;
var
  Slt : PkvStringHashListSlot;
  ICn : Integer;
  I : Integer;
  Itm : PkvStringHashListItem;
begin
  Assert(SlotIdx >= 0);
  Assert(SlotIdx < FSlots);

  Slt := @FList[SlotIdx];
  ICn := Slt^.Count;
  for I := 0 to ICn - 1 do
    begin
      Itm := @Slt^.List[I];
      if SameKey(Itm^.Key, Key, FCaseSensitive) then
        begin
          Item := Itm;
          Result := I;
          exit;
        end;
    end;
  Item := nil;
  Result := -1;
end;

function TkvStringHashList.LocateItemBySlot(const SlotIdx: Integer; const Key: String;
         var Item: PkvStringHashListItem): Boolean;
begin
  Result := LocateItemIndexBySlot(SlotIdx, Key, Item) >= 0;
end;

function TkvStringHashList.LocateItem(const Key: String; var Item: PkvStringHashListItem): Boolean;
var
  Hsh : Word32;
  Slt : Integer;
begin
  Hsh := kvhlHashString(Key, FCaseSensitive);
  Slt := Hsh mod Word32(FSlots);
  Result := LocateItemIndexBySlot(Slt, Key, Item) >= 0;
end;

procedure TkvStringHashList.Add(const Key: String; const Value: TObject);
var
  Hsh : Word32;
  Slt : Integer;
  Itm : PkvStringHashListItem;
begin
  if FCount = FSlots * kvStringHashList_TargetItemsPerSlot then
    ExpandSlots(FSlots * kvStringHashList_SlotExpandFactor);
  Hsh := kvhlHashString(Key, FCaseSensitive);
  Slt := Hsh mod Word32(FSlots);
  if not FAllowDuplicates then
    if LocateItemBySlot(Slt, Key, Itm) then
      raise EkvStringHashList.CreateFmt('Duplicate key: %s', [Key]);
  AddToSlot(Slt, Key, Value);
  Inc(FCount);
end;

function TkvStringHashList.KeyExists(const Key: String): Boolean;
var
  Itm : PkvStringHashListItem;
begin
  Result := LocateItem(Key, Itm);
end;

function TkvStringHashList.RequireItem(const Key: String): PkvStringHashListItem;
var
  Itm : PkvStringHashListItem;
begin
  if not LocateItem(Key, Itm) then
    raise EkvStringHashList.CreateFmt('Key not found: %s', [Key]);
  Result := Itm;
end;

function TkvStringHashList.GetValue(const Key: String; var Value: TObject): Boolean;
var
  Itm : PkvStringHashListItem;
begin
  if not LocateItem(Key, Itm) then
    begin
      Value := nil;
      Result := False;
    end
  else
    begin
      Value := Itm^.Value;
      Result := True;
    end;
end;

function TkvStringHashList.RequireValue(const Key: String): TObject;
begin
  Result := RequireItem(Key)^.Value;
end;

procedure TkvStringHashList.SetValue(const Key: String; const Value: TObject);
var
  Itm : PkvStringHashListItem;
begin
  Itm := RequireItem(Key);
  if FItemOwner then
    Itm^.Value.Free;
  Itm^.Value := Value;
end;

procedure TkvStringHashList.DeleteKey(const Key: String);
var
  Hsh : Word32;
  SltIdx : Integer;
  ItmIdx : Integer;
  Itm : PkvStringHashListItem;
  Slt : PkvStringHashListSlot;
  AllowDup : Boolean;
  First : Boolean;
  I : Integer;
begin
  Hsh := kvhlHashString(Key, FCaseSensitive);
  SltIdx := Hsh mod Word32(FSlots);
  Slt := @FList[SltIdx];
  AllowDup := FAllowDuplicates;
  First := True;
  repeat
    ItmIdx := LocateItemIndexBySlot(SltIdx, Key, Itm);
    if ItmIdx < 0 then
      if First then
        raise EkvStringHashList.CreateFmt('Key not found: %s', [Key])
      else
        break;
    if FItemOwner then
      Itm^.Value.Free;
    for I := ItmIdx to Slt^.Count - 2 do
      Slt^.List[I] := Slt^.List[I + 1];
    Dec(Slt^.Count);
    Dec(FCount);
    First := False;
  until not AllowDup;
end;

procedure TkvStringHashList.Clear;
var
  I, J : Integer;
  Slt : PkvStringHashListSlot;
  Itm : PkvStringHashListItem;
begin
  if FItemOwner then
    for I := Length(FList) - 1 downto 0 do
      begin
        Slt := @FList[I];
        for J := Slt^.Count - 1 downto 0 do
          begin
            Itm := @Slt^.List[J];
            Itm^.Value.Free;
          end;
      end;
  SetLength(FList, 0);
  FSlots := kvStringHashList_InitialSlots;
  SetLength(FList, FSlots);
  for I := 0 to FSlots - 1 do
    FList[I].Count := 0;
  FCount := 0;
end;

function TkvStringHashList.IterateGetNext(var Iterator: TkvStringHashListIterator): PkvStringHashListItem;
var
  SltI : Integer;
  Slt : PkvStringHashListSlot;
  R : Boolean;
begin
  repeat
    R := False;
    SltI := Iterator.SlotIdx;
    if SltI >= FSlots then
      begin
        Result := nil;
        exit;
      end;
    Slt := @FList[SltI];
    if Iterator.ItemIdx >= Slt^.Count then
      begin
        Inc(Iterator.SlotIdx);
        Iterator.ItemIdx := 0;
        R := True;
      end;
  until not R;
  Result := @Slt^.List[Iterator.ItemIdx];
end;

function TkvStringHashList.IterateFirst(var Iterator: TkvStringHashListIterator): PkvStringHashListItem;
begin
  Iterator.SlotIdx := 0;
  Iterator.ItemIdx := 0;
  Result := IterateGetNext(Iterator);
end;

function TkvStringHashList.IterateNext(var Iterator: TkvStringHashListIterator): PkvStringHashListItem;
begin
  Inc(Iterator.ItemIdx);
  Result := IterateGetNext(Iterator);
end;



end.

