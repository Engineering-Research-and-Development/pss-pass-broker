@echo off
setlocal EnableDelayedExpansion

if "%~2"=="" (
    echo Usage: %~nx0 ^<go_file^> ^<function-config.yaml^>
    exit /b 1
)

:: Get absolute paths
set "GO_FILE_PATH=%~f1"
set "CONFIG_FILE_PATH=%~f2"

:: Get the directory and filename of the GO file
set "GO_DIR=%~dp1"
set "GO_FILENAME=%~nx1"

:: Get the filename of the config
set "CONFIG_FILENAME=%~nx2"

if not exist "!GO_FILE_PATH!" (
    echo Error: !GO_FILE_PATH! does not exist.
    exit /b 1
)
if not exist "!CONFIG_FILE_PATH!" (
    echo Error: !CONFIG_FILE_PATH! does not exist.
    exit /b 1
)

:: Extract function name
set "FUNC_NAME="
for /f "tokens=1,* delims=:" %%A in ('findstr /I /B /R "^name:" "!CONFIG_FILE_PATH!"') do (
    set "VAL=%%B"
    :: Remove quotes
    set "VAL=!VAL:"=!"
    :: Remove spaces
    set "VAL=!VAL: =!"
    set "FUNC_NAME=!VAL!"
)

if "!FUNC_NAME!"=="" (
    echo Error: Could not extract 'name' from !CONFIG_FILE_PATH!.
    exit /b 1
)

echo Function name: !FUNC_NAME!

:: Calculate container paths
set "PROJECT_ROOT=%~dp0"

:: Map Go binary path
set "GO_DIR_REL=!GO_DIR:%PROJECT_ROOT%=!"
set "CONTAINER_DIR=!GO_DIR_REL:pulsar-resources=/pulsar/resources!"
set "CONTAINER_DIR=!CONTAINER_DIR:\=/!"
set "CONTAINER_GO_BIN=!CONTAINER_DIR!!FUNC_NAME!"

:: Map Config path
set "CONFIG_DIR=%~dp2"
set "CONFIG_DIR_REL=!CONFIG_DIR:%PROJECT_ROOT%=!"
set "CONTAINER_CONF_DIR=!CONFIG_DIR_REL:pulsar-resources=/pulsar/resources!"
set "CONTAINER_CONF_DIR=!CONTAINER_CONF_DIR:\=/!"
set "CONTAINER_CONF=!CONTAINER_CONF_DIR!!CONFIG_FILENAME!"

echo Changing directory to !GO_DIR!
pushd "!GO_DIR!"

echo Setting environment variables for Linux compile...
set GOOS=linux
set GOARCH=amd64

echo Initializing go modules...
if not exist "go.mod" (
    go mod init !FUNC_NAME!
)
go mod tidy

echo Compiling !GO_FILENAME!...
go build -o "!FUNC_NAME!" "!GO_FILENAME!"
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Compilation failed.
    popd
    exit /b %ERRORLEVEL%
)
echo [SUCCESS] Compilation successful.

echo Checking if Pulsar function '!FUNC_NAME!' already exists...
docker exec broker bin/pulsar-admin functions list --tenant public --namespace default | findstr /C:"!FUNC_NAME!" >nul
if %ERRORLEVEL% EQU 0 (
    echo [INFO] Function '!FUNC_NAME!' exists. Deleting it first...
    docker exec broker bin/pulsar-admin functions delete --tenant public --namespace default --name "!FUNC_NAME!"
    if %ERRORLEVEL% NEQ 0 (
        echo [ERROR] Failed to delete existing Pulsar function.
        popd
        exit /b %ERRORLEVEL%
    )
    echo [SUCCESS] Existing function deleted.
) else (
    echo [INFO] Function '!FUNC_NAME!' does not exist.
)

echo Creating Pulsar function inside 'broker' container...
docker exec broker bin/pulsar-admin functions create --go "!CONTAINER_GO_BIN!" --function-config-file "!CONTAINER_CONF!" --name "!FUNC_NAME!"
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to create Pulsar function.
    popd
    exit /b %ERRORLEVEL%
)
echo [SUCCESS] Pulsar function created.

echo Checking Pulsar function status...
docker exec broker bin/pulsar-admin functions status --name "!FUNC_NAME!" --tenant public --namespace default

popd
endlocal
exit /b 0