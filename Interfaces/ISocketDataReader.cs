using System;

namespace DC_0003.Core.Interfaces;

public interface ISocketDataReader : IDataReader, IDisposable
{
    void Start(string source);

    void Stop();
}
