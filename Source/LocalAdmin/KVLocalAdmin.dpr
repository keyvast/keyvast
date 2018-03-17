{ KeyVast Local Admin }
{ Copyright (c) 2018 KeyVast, David J Butler }
{ KeyVast is released under the terms of the MIT license. }

{ Icon from Kameleon Icons http://www.kameleon.pics/ }

{ 2018/02/11  0.01  Initial version  }

program KVLocalAdmin;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  {$IFDEF MSWINDOWS}
  Windows,
  {$ENDIF }
  {$IFDEF MACOS}
  Macapi.CoreFoundation,
  {$ENDIF }
  System.SysUtils,
  SyncObjs,
  kvlaApplication in 'kvlaApplication.pas',
  kvHashList in '..\KVS\kvHashList.pas',
  kvHash in '..\KVS\kvHash.pas',
  kvValues in '..\KVS\kvValues.pas',
  kvStructures in '..\KVS\kvStructures.pas',
  kvFiles in '..\KVS\kvFiles.pas',
  kvObjects in '..\KVS\kvObjects.pas',
  kvScriptContext in '..\KVS\kvScriptContext.pas',
  kvScriptNodes in '..\KVS\kvScriptNodes.pas',
  kvScriptParser in '..\KVS\kvScriptParser.pas',
  kvScriptSystem in '..\KVS\kvScriptSystem.pas',
  kvScriptFunctions in '..\KVS\kvScriptFunctions.pas',
  flcDecimal in '..\Fundamentals\flcDecimal.pas',
  flcInteger in '..\Fundamentals\flcInteger.pas';

type
  EAppError = class(Exception);

var
  ParamHelp : Boolean = False;
  ParamCommand : String;
  ParamPath : String;
  ParamName : String;

procedure ParseParameters;
var
  I : Integer;
  P : String;
  S : String;
  ParamNr : Integer;
begin
  ParamNr := 0;
  for I := 1 to ParamCount do
    begin
      P := ParamStr(I);
      S := Trim(P);
      if S.StartsWith('--') then
        begin
          S := S.Remove(0, 2);
          S := S.ToLower;
          if S = 'help' then
            ParamHelp := True
          else
            raise EAppError.CreateFmt('Unknown parameter: %s', [P])
        end
      else
      if S.StartsWith('-') then
        begin
          S := S.Remove(0, 1);
          S := S.ToLower;
          if S = 'h' then
            ParamHelp := True
          else
            raise EAppError.CreateFmt('Unknown parameter: %s', [P])
        end
      else
        begin
          Inc(ParamNr);
          if ParamNr = 1 then
            begin
              if (S <> 'create') and (S <> 'open') and (S <> 'delete') then
                raise EAppError.Create('Invalid command parameter');
              ParamCommand := S;
            end
          else
          if ParamNr = 2 then
            ParamPath := S
          else
          if ParamNr = 3 then
            ParamName := S
          else
            raise EAppError.Create('Too many parameters');
        end;
    end;
  if ParamNr = 0 then
    ParamHelp := True
  else
  if ParamNr < 3 then
    raise EAppError.Create('Missing parameter');
end;

procedure PrintTitle;
begin
  Writeln('KeyVast Local Admin 1.40');
end;

procedure PrintHelp;
begin
  PrintTitle;
  Writeln;
  Writeln('Usage:');
  Writeln('  KVLocalAdmin [<options>] <command> <path> <name>');
  Writeln;
  Writeln('Commands:');
  Writeln('  create   Create a system');
  Writeln('  open     Open a system');
  Writeln('  delete   Delete a system');
  Writeln;
  Writeln('Options:');
  Writeln('  -h --help   Print help message');
end;

{ TAppServices }

type
  TAppServices = class
    FLock : TCriticalSection;
    constructor Create;
    destructor Destroy; override;
    procedure AppPrint(Sender: TkvlaApplication; const Txt: String);
  end;

var
  AppServices : TAppServices = nil;
  AppTerminated : Boolean = False;

constructor TAppServices.Create;
begin
  inherited Create;
  FLock := TCriticalSection.Create;
end;

destructor TAppServices.Destroy;
begin
  FreeAndNil(FLock);
  inherited Destroy;
end;

procedure TAppServices.AppPrint(Sender: TkvlaApplication; const Txt: String);
begin
  if Assigned(FLock) then
    FLock.Acquire;
  try
    Writeln(Txt);
  finally
    if Assigned(FLock) then
      FLock.Release;
  end;
end;

procedure AppLock;
begin
  if Assigned(AppServices) and Assigned(AppServices.FLock) then
    AppServices.FLock.Acquire;
end;

procedure AppUnlock;
begin
  if Assigned(AppServices) and Assigned(AppServices.FLock) then
    AppServices.FLock.Release;
end;

{$IFDEF MSWINDOWS}
function ConsoleCtrlHandlerRoutine(const dwCtrlType: LongWord): Boolean; stdcall;
begin
  AppLock;
  AppTerminated := True;
  AppUnlock;
  Result := True;
end;
{$ENDIF}

function GetAppTerminated: Boolean;
begin
  AppLock;
  Result := AppTerminated;
  AppUnlock;
end;

var
  Fin : Boolean;
  CmdS, CmdP : String;

begin
  {$IFDEF DEBUG}
  {$IFDEF TEST}
  flcInteger.Test;
  flcDecimal.Test;
  {$ENDIF}
  {$ENDIF}
  try
    ParseParameters;
    if ParamHelp then
      begin
        PrintHelp;
        exit;
      end;
    AppServices := TAppServices.Create;
    App := TkvlaApplication.Create;
    try
      {$IFDEF MSWINDOWS}
      SetConsoleCtrlHandler(@ConsoleCtrlHandlerRoutine, True);
      {$ENDIF}
      App.OnPrint := AppServices.AppPrint;
      App.Path := ParamPath;
      App.Name := ParamName;
      PrintTitle;
      if ParamCommand = 'delete' then
        App.DeleteSystem
      else
      if ParamCommand = 'create' then
        begin
          App.CreateSystem;
          App.CloseSystem;
        end
      else
      if ParamCommand = 'open' then
        begin
          App.OpenSystem;
          Fin := False;
          repeat
            {$IFDEF MSWINDOWS}
            Sleep(1); // Allow ConsoleCtrlHandlerRoutine to be called
            {$ENDIF}
            if GetAppTerminated then
              break;
            try
              Write(App.GetCommandPrompt + '>');
              if GetAppTerminated then
                break;
              Readln(CmdS);
            except
              on E : EControlC do
                Fin := True;
            end;
            if Fin or GetAppTerminated then
              break;
            CmdP := LowerCase(Trim(CmdS));
            if CmdP <> '' then
              if CmdP = 'exit' then
                Fin := True
              else
                try
                  App.ExecCommand(CmdS);
                except
                  on E : Exception do
                    Writeln('Error: ' + E.Message);
                end;
          until Fin;
          App.CloseSystem;
        end;
    finally
      FreeAndNil(App);
      FreeAndNil(AppServices);
    end;
  except
    on E: Exception do
      Writeln('Error: ' + E.Message);
  end;
end.

