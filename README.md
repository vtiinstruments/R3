# R3 for PowerDAQ

This is VTI Instruments' fork of [Cysharp/R3](https://github.com/Cysharp/R3), based on upstream
tag `1.3.1`. The upstream MIT license and attribution are retained in [LICENSE](LICENSE).

## Supported scope

- .NET 10 or later; the projects currently target `net10.0`.
- Core R3 and WPF (`net10.0-windows`) only.
- The WPF sample and core tests are included. Other platform integrations and samples are removed.

The public APIs, scalar JSON converters, strong-name key and `1.3.1.0` assembly identity are retained
so PowerDAQ can use the fork without migrating its property declarations or saved configurations.

## Reactive-property concurrency

`ReactiveProperty<T>` serializes `Value` writes, `OnNext`, `ForceNotify`, error/completion
notifications, subscription replay/registration and disposal notifications on the property's monitor.
`Value` and `CurrentValue` reads use atomically published snapshots, including for multiword structs,
without taking that monitor. Each update allocates a snapshot. Source-backed read-only properties
and `BindableReactiveProperty<T>` inherit the core synchronization; the existing
`SynchronizedReactiveProperty<T>` remains compatible.

Important boundaries:

- Notifications are synchronous on the calling thread. Marshal WPF work to the dispatcher.
- Bindable validation configuration and UI binding still belong to the UI thread.
- Subscribers must not synchronously wait for another thread that needs the same property.
  Cyclic writes between properties require consistent lock ordering.
- Use `property.Update(value => value + 1)` for atomic read/modify/write operations. It runs the
  transformation and normal setter/comparer under the same property monitor. Do not mutate the
  property from the transformation or block waiting for another writer.
- The C# `Value++` expression still calls the getter and setter separately; use `Update` instead.
  Updates across properties and mutations of reference-type values need caller synchronization.
- The protected `GetValueRef` API bypasses synchronization. Derived callers must provide exclusion.
- Other subjects and operators retain their upstream concurrency requirements.
- Calls after disposal retain R3's `ObjectDisposedException` behavior. Stop producers before tearing
  down a source-backed pipeline. Source-subscription cleanup runs outside the property monitor.

## Build and test

Use Visual Studio MSBuild with the .NET 10 SDK and desktop workloads:

```powershell
msbuild R3.sln /restore /t:Build /p:Configuration=Release /bl:r3-build.binlog
dotnet test tests\R3.Tests\R3.Tests.csproj -c Release --no-build --no-restore /bl:r3-tests.binlog
```

The solution includes only the core, WPF, WPF sample and tests. CI builds this same solution on Windows
with .NET 10. The manual release workflow creates core/WPF packages as artifacts and does not publish
under the upstream owner's NuGet identity.

See the [factory](docs/reference_factory.md) and [operator](docs/reference_operator.md) references for
the remaining core API.
