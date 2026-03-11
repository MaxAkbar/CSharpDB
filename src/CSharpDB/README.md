# CSharpDB

All-in-one package for the [CSharpDB](https://github.com/MaxAkbar/CSharpDB) embedded database.

[![NuGet](https://img.shields.io/nuget/v/CSharpDB)](https://www.nuget.org/packages/CSharpDB)
[![.NET 10](https://img.shields.io/badge/.NET-10-512bd4)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![Release](https://img.shields.io/github/v/release/MaxAkbar/CSharpDB?display_name=tag&label=Release)](https://github.com/MaxAkbar/CSharpDB/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE)

## Overview

`CSharpDB` is the recommended entry package for application developers. It pulls in the full CSharpDB library set:

- Unified client (`CSharpDB.Client`)
- Engine API (`CSharpDB.Engine`)
- ADO.NET provider (`CSharpDB.Data`)
- Deprecated compatibility facade (`CSharpDB.Service`, planned removal in `v2.0.0`)
- Diagnostics (`CSharpDB.Storage.Diagnostics`)
- Required internal dependencies (`CSharpDB.Primitives`, `Sql`, `Storage`, `Execution`)

## Installation

```bash
dotnet add package CSharpDB
```

## License

MIT - see [LICENSE](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE).
