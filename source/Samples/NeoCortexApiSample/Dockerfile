#See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS base
USER app
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Samples/NeoCortexApiSample/NeoCortexApiSample.csproj", "Samples/NeoCortexApiSample/"]
COPY ["NeoCortexApi/NeoCortexApi.csproj", "NeoCortexApi/"]
COPY ["NeoCortexArrayLib/NeoCortexArrayLib.csproj", "NeoCortexArrayLib/"]
COPY ["NeoCortexEntities/NeoCortexEntities.csproj", "NeoCortexEntities/"]
COPY ["NeoCortexUtils/NeoCortexUtils.csproj", "NeoCortexUtils/"]
COPY ["GridCell/GridCell.csproj", "GridCell/"]
RUN dotnet restore "./Samples/NeoCortexApiSample/NeoCortexApiSample.csproj"
COPY . .
WORKDIR "/src/Samples/NeoCortexApiSample"
RUN dotnet build "./NeoCortexApiSample.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./NeoCortexApiSample.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "NeoCortexApiSample.dll"]