FROM mcr.microsoft.com/dotnet/aspnet:9.0-alpine AS base
RUN apk --no-cache add tzdata
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:9.0-alpine AS build
WORKDIR /src
COPY ["Core.TaskProcessor.SampleWebApi/Core.TaskProcessor.SampleWebApi.csproj", "Core.TaskProcessor.SampleWebApi/"]
RUN dotnet restore "Core.TaskProcessor.SampleWebApi/Core.TaskProcessor.SampleWebApi.csproj"
COPY . .
WORKDIR "/src/Core.TaskProcessor.SampleWebApi"
RUN dotnet build "Core.TaskProcessor.SampleWebApi.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Core.TaskProcessor.SampleWebApi.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Core.TaskProcessor.SampleWebApi.dll"]