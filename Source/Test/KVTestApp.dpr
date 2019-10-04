program KVTestApp;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils,
  flcInteger in '..\Fundamentals\flcInteger.pas',
  flcDecimal in '..\Fundamentals\flcDecimal.pas',
  kvHashList in '..\KVS\kvHashList.pas',
  kvValues in '..\KVS\kvValues.pas',
  kvAbstractSystem in '..\KVS\kvAbstractSystem.pas',
  kvBaseSystem in '..\KVS\kvBaseSystem.pas',
  kvDiskHash in '..\KVS\kvDiskHash.pas',
  kvDiskFileStructures in '..\KVS\kvDiskFileStructures.pas',
  kvDiskFiles in '..\KVS\kvDiskFiles.pas',
  kvDiskSystem in '..\KVS\kvDiskSystem.pas',
  kvScriptContext in '..\KVS\kvScriptContext.pas',
  kvScriptFunctions in '..\KVS\kvScriptFunctions.pas',
  kvScriptNodes in '..\KVS\kvScriptNodes.pas',
  kvScriptParser in '..\KVS\kvScriptParser.pas',
  kvScriptSystem in '..\KVS\kvScriptSystem.pas',
  kvDatasysClient in '..\KVS\kvDatasysClient.pas',
  kvDatasysServer in '..\KVS\kvDatasysServer.pas',
  kvTest in '..\KVS\kvTest.pas';

begin
  try
    flcInteger.Test;
    flcDecimal.Test;
    kvTest.Test;
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
end.
