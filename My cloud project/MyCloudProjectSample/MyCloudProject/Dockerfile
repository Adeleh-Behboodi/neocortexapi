#See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS base
USER app
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["MyCloudProject/MyCloudProject.csproj", "MyCloudProject/"]
COPY ["MyCloudProject.Common/MyCloudProject.Common.csproj", "MyCloudProject.Common/"]
COPY ["MyExperiment/MyExperiment.csproj", "MyExperiment/"]
RUN dotnet restore "./MyCloudProject/MyCloudProject.csproj"
COPY . .
WORKDIR "/src/MyCloudProject"
RUN dotnet build "./MyCloudProject.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./MyCloudProject.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "MyCloudProject.dll"]