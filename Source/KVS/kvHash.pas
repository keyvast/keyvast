{ KeyVast - A key value store }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/02/09  0.01  Initial version, 64 bit hash }
{ 2018/03/02  0.02  Fix Level1HashString for case insensitve keys }

{$INCLUDE kvInclude.inc}

unit kvHash;

interface



function kvLevel1HashString(const S: String; const CaseSensitive: Boolean): UInt64;
function kvLevelNHash(const PrevLevelHash: UInt64): UInt64;



implementation

uses
  SysUtils;



type
  Word32 = FixedUInt;

const
  RC2Table: array[Byte] of Byte = (
    $D9, $78, $F9, $C4, $19, $DD, $B5, $ED, $28, $E9, $FD, $79, $4A, $A0, $D8, $9D,
    $C6, $7E, $37, $83, $2B, $76, $53, $8E, $62, $4C, $64, $88, $44, $8B, $FB, $A2,
    $17, $9A, $59, $F5, $87, $B3, $4F, $13, $61, $45, $6D, $8D, $09, $81, $7D, $32,
    $BD, $8F, $40, $EB, $86, $B7, $7B, $0B, $F0, $95, $21, $22, $5C, $6B, $4E, $82,
    $54, $D6, $65, $93, $CE, $60, $B2, $1C, $73, $56, $C0, $14, $A7, $8C, $F1, $DC,
    $12, $75, $CA, $1F, $3B, $BE, $E4, $D1, $42, $3D, $D4, $30, $A3, $3C, $B6, $26,
    $6F, $BF, $0E, $DA, $46, $69, $07, $57, $27, $F2, $1D, $9B, $BC, $94, $43, $03,
    $F8, $11, $C7, $F6, $90, $EF, $3E, $E7, $06, $C3, $D5, $2F, $C8, $66, $1E, $D7,
    $08, $E8, $EA, $DE, $80, $52, $EE, $F7, $84, $AA, $72, $AC, $35, $4D, $6A, $2A,
    $96, $1A, $D2, $71, $5A, $15, $49, $74, $4B, $9F, $D0, $5E, $04, $18, $A4, $EC,
    $C2, $E0, $41, $6E, $0F, $51, $CB, $CC, $24, $91, $AF, $50, $A1, $F4, $70, $39,
    $99, $7C, $3A, $85, $23, $B8, $B4, $7A, $FC, $02, $36, $5B, $25, $55, $97, $31,
    $2D, $5D, $FA, $98, $E3, $8A, $92, $AE, $05, $DF, $29, $10, $67, $6C, $BA, $C9,
    $D3, $00, $E6, $CF, $E1, $9E, $A8, $2C, $63, $16, $01, $3F, $58, $E2, $89, $A9,
    $0D, $38, $34, $1B, $AB, $33, $FF, $B0, $BB, $48, $0C, $5F, $B9, $B1, $CD, $2E,
    $C5, $F3, $DB, $47, $E5, $A5, $9C, $77, $0A, $A6, $20, $68, $FE, $7F, $C1, $AD);

function TransformHash(const Hash: UInt64): UInt64;
var
  H : UInt64;
  P : PByte;
  I : Integer;
begin
  H := Hash;
  P := @H;
  for I := 0 to SizeOf(UInt64) - 1 do
    begin
      P^ := RC2Table[P^];
      Inc(P);
    end;
  Result := H;
end;

function kvLevel1HashString(const S: String; const CaseSensitive: Boolean): UInt64;
var
  H : UInt64;
  L : Integer;
  I : Integer;
  C : WideChar;
  D : UInt64;
  F : UInt64;
  H1, H2 : Word32;
  T1, T2 : Word32;
begin
  H := $5A1F7301B3E05962;
  L := Length(S);
  for I := 1 to L do
    begin
      C := S[I];
      D := Ord(C);
      if not CaseSensitive then
        if (D >= Ord('a')) and (D <= Ord('z')) then
          D := D - 32;

      F := D xor (D shl 16) xor (D shl 32) xor (D shl 48);
      H := H xor F;
      F := L xor (Int64(I) shl 18) xor (Int64(L) shl 28) xor (Int64(I) shl 31);
      H := H xor F;

      H := TransformHash(H);

      H1 := Word32(H shr 32);
      H2 := Word32(H and $FFFFFFFF);
      H1 := Word32(H1 + Word32(I) + Word32(D));
      H2 := Word32(H2 + Word32(I) + Word32(L));
      H1 := Word32(Int64(H1) * 73 + 1);
      H2 := Word32(Int64(H2) * 5 + 79);

      T1 := Word32(H1 shl 11) xor (H1 shr 5) xor Word32(H2 shl 17) xor (H2 shr 19);
      T2 := Word32(H2 shl 7)  xor (H2 shr 3) xor Word32(H1 shl 15) xor (H1 shr 17);
      H := (UInt64(T1) shl 32) or T2;
    end;

  Result := H;
end;

function kvLevelNHash(const PrevLevelHash: UInt64): UInt64;
var
  H : UInt64;
begin
  H := TransformHash(PrevLevelHash);
  H := UInt64(H shl 17) or (H shr 47);
  Result := H;
end;



end.

