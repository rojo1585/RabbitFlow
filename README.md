<p align="center">
  <img src="https://raw.githubusercontent.com/dotnet/brand/main/logo/dotnet-logo.svg" alt="Apymsa.RabbitMQ Logo" width="100" height="100" />
  <h1 align="center">Apymsa.RabbitMQ</h1>
  <p align="center">
    <b>Enterprise-grade, high-throughput RabbitMQ client framework for .NET</b>
  </p>
  <p align="center">
    <a href="#-features">Features</a> •
    <a href="#-architecture">Architecture</a> •
    <a href="#-quick-start">Quick Start</a> •
    <a href="#-channel-pooling">Channel Pooling</a> •
    <a href="#-observability">Observability</a>
  </p>
</p>

<p align="center">
  <a href="https://dotnet.microsoft.com/"><img src="https://img.shields.io/badge/.NET-8.0%20%7C%209.0%20%7C%2010.0%20%7C%20Standard%202.0-512BD4?style=for-the-badge&logo=dotnet&logoColor=white" alt=".NET Support" /></a>
  <a href="https://www.nuget.org/packages/Apymsa.RabbitMQ"><img src="https://img.shields.io/nuget/v/Apymsa.RabbitMQ?style=for-the-badge&logo=nuget&color=004880" alt="NuGet Version" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge" alt="License: MIT" /></a>
  <a href="https://opentelemetry.io/"><img src="https://img.shields.io/badge/OpenTelemetry-Enabled-008080?style=for-the-badge&logo=opentelemetry&logoColor=white" alt="OpenTelemetry Ready" /></a>
</p>

---

## ⚡ Features

| Feature | Description |
| :--- | :--- |
| 🌐 **Multi-Broker Support** | Connect to multiple RabbitMQ brokers, clusters, or vhosts simultaneously within a single app. |
| ⚡ **Channel Pooling** | High-throughput producer channel reuse using `SemaphoreSlim` backpressure — eliminates *channel-per-publish* overhead. |
| 📦 **Batch Consumer** | Buffer high-volume messages in-memory and execute bulk processing with size/timeout triggers. |
| 🔄 **Event Versioning** | Transparently upgrade legacy event schemas to latest contracts via `IEventUpgrader<TFrom, TTo>` chains. |
| 🛡️ **Resilience & DLQ** | Built-in retry strategies with exponential backoff and automatic Dead-Letter Queue (DLQ) topology setup. |
| 📊 **Native OpenTelemetry** | Out-of-the-box distributed tracing (`ActivitySource`) and custom metrics (`Meter`). |
| 🩺 **Health Checks** | Native ASP.NET Core health check integration for individual connections and aggregate cluster health. |

---

## 🏗️ Architecture

```mermaid
flowchart TD
    subgraph Publisher App
        P[Publisher Service] -->|Rent Channel| CP[Channel Pool]
        CP -->|Publish Message| EX[RabbitMQ Exchange]
    end

    subgraph RabbitMQ Broker
        EX -->|Route| Q1[Queue: orders.created]
        EX -->|Route| Q2[Queue: notifications.pending]
    end

    subgraph Consumer App
        Q1 -->|Consume| SC[Single Consumer]
        Q2 -->|Buffer| BC[Batch Consumer]
        
        SC -->|Dispatch| H1[OrderCreatedHandler]
        BC -->|Flush Batch| H2[NotificationBatchHandler]
    end