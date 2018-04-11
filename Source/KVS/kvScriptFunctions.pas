{ KeyVast - A key value store }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ 2018/03/01  0.01  Initial version }
{ 2018/03/03  0.02  Additional string functions }
{ 2018/03/12  0.03  Change result owner from class to caller }

{$INCLUDE kvInclude.inc}

unit kvScriptFunctions;

interface

uses
  SysUtils,
  kvValues,
  kvScriptContext;



type
  EkvScriptFunction = class(Exception);

  { Procedure value }

  AkvScriptProcedureValue = class
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; virtual; abstract;
  end;

  { Length built-in function }

  TkvScriptLengthBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Integer cast built-in function }

  TkvScriptIntegerCastBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Float cast built-in function }

  TkvScriptFloatCastBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { String cast built-in function }

  TkvScriptStringCastBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { DateTime cast built-in function }

  TkvScriptDateTimeCastBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Date built-in function }

  TkvScriptDateBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Time built-in function }

  TkvScriptTimeBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Byte cast built-in function }

  TkvScriptByteCastBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Binary cast built-in function }

  TkvScriptBinaryCastBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Char cast built-in function }

  TkvScriptCharCastBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Decimal cast built-in function }

  TkvScriptDecimalCastBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Replace built-in function }

  TkvScriptReplaceBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { GetDate built-in function }

  TkvScriptGetDateBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { GetTimestamp built-in function }

  TkvScriptGetTimestampBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { IsNull built-in function }

  TkvScriptIsNullBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Lower built-in function }

  TkvScriptLowerBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Upper built-in function }

  TkvScriptUpperBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Trim built-in function }

  TkvScriptTrimBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Round built-in function }

  TkvScriptRoundBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Substring built-in function }

  TkvScriptSubstringBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { IndexOf built-in function }

  TkvScriptIndexOfBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Left built-in function }

  TkvScriptLeftBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { Right built-in function }

  TkvScriptRightBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;

  { SetOf built-in function }

  TkvScriptSetOfBuiltInFunction = class(AkvScriptProcedureValue)
  public
    function  Call(const Context: TkvScriptContext;
              const ParamValues: TkvValueArray): AkvValue; override;
  end;



implementation

uses
  {$IFDEF MACOS}
  Macapi.CoreFoundation,
  {$ENDIF}
  StrUtils,
  flcDecimal;



{ TkvScriptLengthBuiltInFunction }

function TkvScriptLengthBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  V : AkvValue;
  L : Integer;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  V := ParamValues[0];
  if V is TkvListValue then
    L := TkvListValue(V).GetCount
  else
  if V is TkvStringValue then
    L := Length(TkvStringValue(V).AsString)
  else
  if V is TkvBinaryValue then
    L := Length(TkvBinaryValue(V).AsBinary)
  else
    raise EkvScriptFunction.Create('Invalid parameter type');
  Result := TkvIntegerValue.Create(L);
end;



{ TkvScriptIntegerCastBuiltInFunction }

function TkvScriptIntegerCastBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  V : AkvValue;
  L : Int64;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  V := ParamValues[0];
  L := V.AsInteger;
  Result := TkvIntegerValue.Create(L);
end;



{ TkvScriptFloatCastBuiltInFunction }

function TkvScriptFloatCastBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  V : AkvValue;
  L : Double;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  V := ParamValues[0];
  L := V.AsFloat;
  Result := TkvFloatValue.Create(L);
end;



{ TkvScriptStringCastBuiltInFunction }

function TkvScriptStringCastBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  V : AkvValue;
  S : String;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  V := ParamValues[0];
  S := V.AsString;
  Result := TkvStringValue.Create(S);
end;



{ TkvScriptDateTimeCastBuiltInFunction }

function TkvScriptDateTimeCastBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  V : AkvValue;
  D : TDateTime;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  V := ParamValues[0];
  D := V.AsDateTime;
  Result := TkvDateTimeValue.Create(D);
end;



{ TkvScriptDateBuiltInFunction }

function TkvScriptDateBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  Ye, Mo, Da : Int64;
  D : TDateTime;
begin
  if Length(ParamValues) <> 3 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  Ye := ParamValues[0].AsInteger;
  Mo := ParamValues[1].AsInteger;
  Da := ParamValues[2].AsInteger;
  D := EncodeDate(Ye, Mo, Da);
  Result := TkvDateTimeValue.Create(D);
end;



{ TkvScriptTimeBuiltInFunction }

function TkvScriptTimeBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  L : Integer;
  Ho, Mi, Se, ZZ : Int64;
  D : TDateTime;
begin
  L := Length(ParamValues);
  if (L < 3) or (L > 4) then
    raise EkvScriptFunction.Create('Invalid parameter count');
  Ho := ParamValues[0].AsInteger;
  Mi := ParamValues[1].AsInteger;
  Se := ParamValues[2].AsInteger;
  if L > 3 then
    ZZ := ParamValues[3].AsInteger
  else
    ZZ := 0;
  D := EncodeTime(Ho, Mi, Se, ZZ);
  Result := TkvDateTimeValue.Create(D);
end;



{ TkvScriptByteCastBuiltInFunction }

function TkvScriptByteCastBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  V : AkvValue;
  B : Int64;
  A : kvByteArray;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  V := ParamValues[0];
  B := V.AsInteger;
  if B > $FF then
    raise EkvScriptFunction.Create('Invalid byte value');
  SetLength(A, 1);
  A[0] := Byte(B);
  Result := TkvBinaryValue.Create(A);
end;



{ TkvScriptBinaryCastBuiltInFunction }

function TkvScriptBinaryCastBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  V : AkvValue;
  B : kvByteArray;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  V := ParamValues[0];
  B := V.AsBinary;
  Result := TkvBinaryValue.Create(B);
end;



{ TkvScriptCharCastBuiltInFunction }

function TkvScriptCharCastBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  V : AkvValue;
  B : Int64;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  V := ParamValues[0];
  B := V.AsInteger;
  if B > $FFFF then
    raise EkvScriptFunction.Create('Invalid character value');
  Result := TkvStringValue.Create(WideChar(B));
end;



{ TkvScriptDecimalCastBuiltInFunction }

function TkvScriptDecimalCastBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  V : AkvValue;
  B : SDecimal128;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  V := ParamValues[0];
  B := V.AsDecimal128;
  Result := TkvDecimal128Value.Create(B);
end;



{ TkvScriptReplaceBuiltInFunction }

function TkvScriptReplaceBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  VS, VF, VR : AkvValue;
  SS, SF, SR : String;
  S : String;
begin
  if Length(ParamValues) <> 3 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  VS := ParamValues[0];
  VF := ParamValues[1];
  VR := ParamValues[2];
  SS := VS.AsString;
  SF := VF.AsString;
  SR := VR.AsString;
  S := ReplaceText(SS, SF, SR);
  Result := TkvStringValue.Create(S);
end;



{ TkvScriptGetDateBuiltInFunction }

function TkvScriptGetDateBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
begin
  if Length(ParamValues) <> 0 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  Result := TkvDateTimeValue.Create(Now);
end;



{ TkvScriptGetTimestampBuiltInFunction }

function TkvScriptGetTimestampBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  N : TDateTime;
  T : PInt64;
begin
  if Length(ParamValues) <> 0 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  N := Now;
  T := @N;
  Result := TkvIntegerValue.Create(T^);
end;



{ TkvScriptIsNullBuiltInFunction }

function TkvScriptIsNullBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  V1 : AkvValue;
  V2 : AkvValue;
begin
  if Length(ParamValues) <> 2 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  V1 := ParamValues[0];
  V2 := ParamValues[1];
  if V1 is TkvNullValue then
    Result := V2.Duplicate
  else
    Result := V1.Duplicate;
end;



{ TkvScriptLowerBuiltInFunction }

function TkvScriptLowerBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  S : String;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  S := ParamValues[0].AsString;
  S := S.ToLower;
  Result := TkvStringValue.Create(S);
end;



{ TkvScriptUpperBuiltInFunction }

function TkvScriptUpperBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  S : String;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  S := ParamValues[0].AsString;
  S := S.ToUpper;
  Result := TkvStringValue.Create(S);
end;



{ TkvScriptTrimBuiltInFunction }

function TkvScriptTrimBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  S : String;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  S := ParamValues[0].AsString;
  S := S.Trim;
  Result := TkvStringValue.Create(S);
end;



{ TkvScriptRoundBuiltInFunction }

function TkvScriptRoundBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  F : Double;
begin
  if Length(ParamValues) <> 1 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  F := ParamValues[0].AsFloat;
  Result := TkvIntegerValue.Create(Round(F));
end;



{ TkvScriptSubstringBuiltInFunction }

function TkvScriptSubstringBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  S : String;
  I, L : Integer;
begin
  if Length(ParamValues) <> 3 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  S := ParamValues[0].AsString;
  I := ParamValues[1].AsInteger;
  L := ParamValues[2].AsInteger;
  Result := TkvStringValue.Create(Copy(S, I + 1, L));
end;



{ TkvScriptIndexOfBuiltInFunction }

function TkvScriptIndexOfBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  S, T : String;
begin
  if Length(ParamValues) <> 2 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  S := ParamValues[0].AsString;
  T := ParamValues[1].AsString;
  Result := TkvIntegerValue.Create(T.IndexOf(S));
end;



{ TkvScriptLeftBuiltInFunction }

function TkvScriptLeftBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  S : String;
  N : Integer;
begin
  if Length(ParamValues) <> 2 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  S := ParamValues[0].AsString;
  N := ParamValues[1].AsInteger;
  Result := TkvStringValue.Create(Copy(S, 1, N));
end;



{ TkvScriptRightBuiltInFunction }

function TkvScriptRightBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  S : String;
  N, L : Integer;
begin
  if Length(ParamValues) <> 2 then
    raise EkvScriptFunction.Create('Invalid parameter count');
  S := ParamValues[0].AsString;
  N := ParamValues[1].AsInteger;
  L := Length(S);
  Result := TkvStringValue.Create(Copy(S, L - N + 1, N));
end;



{ TkvScriptSetOfBuiltInFunction }

function TkvScriptSetOfBuiltInFunction.Call(const Context: TkvScriptContext;
         const ParamValues: TkvValueArray): AkvValue;
var
  Res : TkvSetValue;
  L : Integer;
  V : AkvValue;
  Li : TkvListValue;
  I, PaI : Integer;
begin
  L := Length(ParamValues);
  Res := TkvSetValue.Create;
  for PaI := 0 to L - 1 do
    begin
      V := ParamValues[PaI];
      if V is TkvListValue then
        begin
          Li := TkvListValue(V);
          for I := 0 to Li.GetCount - 1 do
            Res.Add(Li.GetValue(I).AsString);
        end
      else
      if V is TkvStringValue then
        Res.Add(V.AsString)
      else
        raise EkvScriptFunction.Create('Invalid parameter type: List or string expected');
    end;
  Result := Res;
end;



end.

