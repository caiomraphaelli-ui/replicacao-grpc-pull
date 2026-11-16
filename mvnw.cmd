@echo off
rem ------------------------------------------------------------------
rem Maven Wrapper simplificado: baixa o Apache Maven na primeira vez
rem (em %USERPROFILE%\.m2\wrapper) e executa. Nao precisa instalar Maven.
rem Requer: JDK 17+ (JAVA_HOME definido ou java no PATH).
rem ------------------------------------------------------------------
setlocal
set "MVN_VERSION=3.9.11"
set "MVN_BASE=%USERPROFILE%\.m2\wrapper\dists\apache-maven-%MVN_VERSION%"
set "MVN_CMD=%MVN_BASE%\apache-maven-%MVN_VERSION%\bin\mvn.cmd"
set "MVN_URL=https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/%MVN_VERSION%/apache-maven-%MVN_VERSION%-bin.zip"

if exist "%MVN_CMD%" goto run

echo Baixando Apache Maven %MVN_VERSION% - apenas na primeira execucao...
powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='Stop'; [Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12; $d=$env:MVN_BASE; New-Item -ItemType Directory -Force -Path $d | Out-Null; $z=Join-Path $d 'maven.zip'; Invoke-WebRequest -UseBasicParsing -Uri $env:MVN_URL -OutFile $z; Expand-Archive -Force -Path $z -DestinationPath $d; Remove-Item $z"
if errorlevel 1 goto fail
if not exist "%MVN_CMD%" goto fail
echo Maven instalado em %MVN_BASE%

:run
call "%MVN_CMD%" %*
endlocal & exit /b %ERRORLEVEL%

:fail
echo ERRO: nao foi possivel baixar o Maven de %MVN_URL%
endlocal & exit /b 1
