#if NET6_0_OR_GREATER
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
#endif

namespace R3;

public abstract class ReadOnlyReactiveProperty<T> : Observable<T>, IDisposable
{
    public abstract T CurrentValue { get; }
    protected virtual void OnValueChanged(T value) { }
    protected virtual void OnReceiveError(Exception exception) { }
    public ReadOnlyReactiveProperty<T> ToReadOnlyReactiveProperty() => this;
    public abstract void Dispose();
}

// allow inherit

#if NET6_0_OR_GREATER
[JsonConverter(typeof(ReactivePropertyJsonConverterFactory))]
#endif
public class ReactiveProperty<T> : ReadOnlyReactiveProperty<T>, ISubject<T>
{
    const byte NotCompleted = 0;
    const byte CompletedSuccess = 1;
    const byte CompletedFailure = 2;
    const byte Disposed = 3;

    byte completeState;
    Exception? error;
    ValueSnapshot currentValue = new(default!);
    IEqualityComparer<T>? equalityComparer; // allow null for no-compare

    // Publish whole values without taking the notification lock on readers. A locking getter would
    // invert locks in consumers that read a property while holding their own state lock.
    sealed class ValueSnapshot(T value)
    {
        public T Value = value;
    }

    // For reduce memory usage, ReactiveProperty<T> itself is LinkedList and subscription represents LinkedListNode.
    // The last of node is root.Previous(if null, single list).
    ObserverNode? root;

    public IEqualityComparer<T>? EqualityComparer => equalityComparer;

    public override T CurrentValue => Volatile.Read(ref currentValue).Value;

    public bool HasObservers => Volatile.Read(ref root) != null;
    public bool IsCompleted => Volatile.Read(ref completeState) is CompletedSuccess or CompletedFailure;
    public bool IsDisposed => Volatile.Read(ref completeState) == Disposed;
    public bool IsCompletedOrDisposed => IsCompleted || IsDisposed;

    public virtual T Value
    {
        get => CurrentValue;
        set
        {
            lock (this)
            {
                OnValueChanging(ref value);

                if (EqualityComparer != null)
                {
                    if (EqualityComparer.Equals(CurrentValue, value))
                    {
                        return;
                    }
                }

                Volatile.Write(ref currentValue, new ValueSnapshot(value));
                OnValueChanged(value);

                OnNextCore(value);
            }
        }
    }

    public ReactiveProperty() : this(default!)
    {
    }

    public ReactiveProperty(T value)
        : this(value, EqualityComparer<T>.Default)
    {
    }

    public ReactiveProperty(T value, IEqualityComparer<T>? equalityComparer)
    {
        this.equalityComparer = equalityComparer;
        OnValueChanging(ref value);
        this.currentValue = new ValueSnapshot(value);
        OnValueChanged(value);
    }

    protected ReactiveProperty(T value, IEqualityComparer<T>? equalityComparer, bool callOnValueChangeInBaseConstructor)
    {
        this.equalityComparer = equalityComparer;

        if (callOnValueChangeInBaseConstructor)
        {
            OnValueChanging(ref value);
        }

        this.currentValue = new ValueSnapshot(value);

        if (callOnValueChangeInBaseConstructor)
        {
            OnValueChanged(value);
        }
    }

    protected virtual void OnValueChanging(ref T value) { }
    protected ref T GetValueRef() => ref currentValue.Value; // dangerous: ref writes bypass synchronization

    public virtual void ForceNotify()
    {
        lock (this)
        {
            OnNext(Value);
        }
    }

    public virtual void OnNext(T value)
    {
        lock (this)
        {
            OnValueChanging(ref value);
            Volatile.Write(ref currentValue, new ValueSnapshot(value));
            OnValueChanged(value);

            OnNextCore(value);
        }
    }

    protected virtual void OnNextCore(T value)
    {
        lock (this)
        {
            ThrowIfDisposed();
            if (IsCompleted) return;

            var node = root;
            var last = node?.Previous;
            while (node != null)
            {
                node.Observer.OnNext(value);
                if (node == last) return;
                node = node.Next;
            }
        }
    }

    public void OnErrorResume(Exception error)
    {
        lock (this)
        {
            ThrowIfDisposed();
            if (IsCompleted) return;

            OnReceiveError(error);

            var node = root;
            var last = node?.Previous;
            while (node != null)
            {
                node.Observer.OnErrorResume(error);
                if (node == last) return;
                node = node.Next;
            }
        }
    }

    public void OnCompleted(Result result)
    {
        lock (this)
        {
            ThrowIfDisposed();
            if (IsCompleted) return;

            ObserverNode? node = null;
            if (completeState == NotCompleted)
            {
                Volatile.Write(ref completeState, result.IsSuccess ? CompletedSuccess : CompletedFailure);
                error = result.Exception;
                node = root;
                Volatile.Write(ref root, null); // when complete, List is clear.
            }
            else
            {
                // IsCompleted = do-nothing, IsDisposed = throw
                ThrowIfDisposed();
                return;
            }
            if (result.IsFailure)
            {
                OnReceiveError(result.Exception);
            }

            var last = node?.Previous;
            while (node != null)
            {
                node.Observer.OnCompleted(result);
                if (node == last) return;
                node = node.Next;
            }
        }
    }

    protected override IDisposable SubscribeCore(Observer<T> observer)
    {
        lock (this)
        {
            ThrowIfDisposed();
            if (IsCompleted)
            {
                var result = (error == null) ? Result.Success : Result.Failure(error);
                if (result.IsSuccess)
                {
                    observer.OnNext(CurrentValue);
                }
                observer.OnCompleted(result);
                return Disposable.Empty;
            }

            // Replay and registration share the writer lock so no update can be lost between them.
            observer.OnNext(CurrentValue);

            ThrowIfDisposed();
            if (IsCompleted)
            {
                observer.OnCompleted((error == null) ? Result.Success : Result.Failure(error));
                return Disposable.Empty;
            }

            return new ObserverNode(this, observer);
        }
    }

    void ThrowIfDisposed()
    {
        if (IsDisposed) throw new ObjectDisposedException("");
    }

    public override void Dispose()
    {
        Dispose(true);
    }

    public void Dispose(bool callOnCompleted)
    {
        lock (this)
        {
            ObserverNode? node = null;
            if (completeState == Disposed)
            {
                return;
            }

            // not yet disposed so can call list iteration
            if (callOnCompleted && !IsCompleted)
            {
                node = Volatile.Read(ref root);
            }

            Volatile.Write(ref root, null);
            Volatile.Write(ref completeState, Disposed);

            while (node != null)
            {
                node.Observer.OnCompleted();
                node = node.Next;
            }

        }
        DisposeCore();
    }

    protected virtual void DisposeCore() { }

    public override string? ToString()
    {
        var value = CurrentValue;
        return (value == null) ? "(null)" : value.ToString();
    }

    // debugging property

#if DEBUG

    public int NodeCount
    {
        get
        {
            lock (this)
            {
                var count = 0;
                var node = root;
                while (node != null)
                {
                    count++;
                    node = node.Next;
                }
                return count;
            }
        }
    }

    ObserverNode[] Nodes
    {
        get
        {
            lock (this)
            {
                var list = new List<ObserverNode>();
                var node = root;
                while (node != null)
                {
                    list.Add(node);
                    node = node.Next;
                }
                return list.ToArray();
            }
        }
    }

#endif

    sealed class ObserverNode : IDisposable
    {
        public readonly Observer<T> Observer;

        ReactiveProperty<T>? parent;

        public ObserverNode? Previous { get; set; } // Previous is last node or root(null).
        public ObserverNode? Next { get; set; }


        // for debugging field/property
#if DEBUG
        static int idGenerator;
        public int Id = Interlocked.Increment(ref idGenerator);
        public bool IsRootNode => parent?.root == this;
        public bool IsSingleRootNode => IsRootNode && Previous == null;

        public override string ToString()
        {
            return $"{Previous?.Id} -> ({Id}) -> {Next?.Id}";
        }
#endif

        public ObserverNode(ReactiveProperty<T> parent, Observer<T> observer)
        {
            this.parent = parent;
            this.Observer = observer;

            // Add node(self) to list(ReactiveProperty), called in lock
            if (parent.root == null)
            {
                // Single list(both previous and next is null)
                Volatile.Write(ref parent.root, this);
            }
            else
            {
                // previous is last, null then root is last.
                var lastNode = parent.root.Previous ?? parent.root;

                lastNode.Next = this;
                this.Previous = lastNode;
                parent.root.Previous = this;
            }
        }

        public void Dispose()
        {
            var p = Interlocked.Exchange(ref parent, null);
            if (p == null) return;

            // keep this.Next for dispose on iterating
            // Remove node(self) from list(ReactiveProperty)
            lock (p)
            {
                if (p.IsCompletedOrDisposed) return;

                if (this == p.root)
                {
                    if (this.Previous == null || this.Next == null)
                    {
                        // case of single list
                        p.root = null;
                    }
                    else
                    {
                        // otherwise, root is next node.
                        var root = this.Next;

                        // single list.
                        if (root.Next == null)
                        {
                            root.Previous = null;
                        }
                        else
                        {
                            root.Previous = this.Previous; // as last.
                        }

                        p.root = root;
                    }
                }
                else
                {
                    // node is not root, previous must exists
                    this.Previous!.Next = this.Next;
                    if (this.Next != null)
                    {
                        this.Next.Previous = this.Previous;
                    }
                    else
                    {
                        // next does not exists, previous is last node so modify root
                        p.root!.Previous = this.Previous;
                    }
                }
            }
        }
    }
}

#if NET6_0_OR_GREATER

public class ReactivePropertyJsonConverterFactory : JsonConverterFactory
{
    public override bool CanConvert(Type typeToConvert)
    {
        return GetReactivePropertyType(typeToConvert) != null;
    }

    public override JsonConverter? CreateConverter(Type typeToConvert, JsonSerializerOptions options)
    {
        var t = GetReactivePropertyType(typeToConvert);
        if (t != null)
        {
            var rt = GenericConverterType.MakeGenericType(t.GetGenericArguments()[0]);
            return (JsonConverter?)Activator.CreateInstance(rt, false);
        }
        else
        {
            return null;
        }
    }

    Type? GetReactivePropertyType(Type? type)
    {
        while (type != null)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ReactiveProperty<>))
            {
                return type;
            }

            type = type.BaseType;
        }
        return null;
    }

    // allow customize
    protected virtual Type GenericConverterType => typeof(ReactivePropertyJsonConverter<>);
}

public class ReactivePropertyJsonConverter<T> : JsonConverter<ReactiveProperty<T>>
{
    public override void Write(Utf8JsonWriter writer, ReactiveProperty<T> value, JsonSerializerOptions options)
    {
        JsonSerializer.Serialize(writer, value.Value, options);
    }

    public override ReactiveProperty<T>? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var v = JsonSerializer.Deserialize<T>(ref reader, options);
        return CreateReactiveProperty(v!);
    }

    // allow customize
    protected virtual ReactiveProperty<T> CreateReactiveProperty(T value)
    {
        return new ReactiveProperty<T>(value);
    }

    public override bool CanConvert(Type typeToConvert)
    {
        return GetReactivePropertyType(typeToConvert) != null;
    }

    Type? GetReactivePropertyType(Type? type)
    {
        while (type != null)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ReactiveProperty<>))
            {
                return type;
            }

            type = type.BaseType;
        }
        return null;
    }
}

// for BindableReactiveProperty

internal class BindableReactivePropertyJsonConverterFactory : ReactivePropertyJsonConverterFactory
{
    protected override Type GenericConverterType => typeof(BindableReactivePropertyJsonConverter<>);
}

internal class BindableReactivePropertyJsonConverter<T> : ReactivePropertyJsonConverter<T>
{
    protected override ReactiveProperty<T> CreateReactiveProperty(T value)
    {
        return new BindableReactiveProperty<T>(value);
    }
}

// for SynchronizedReactiveProperty

internal class SynchronizedReactivePropertyJsonConverterFactory : ReactivePropertyJsonConverterFactory
{
    protected override Type GenericConverterType => typeof(SynchronizedReactivePropertyJsonConverter<>);
}

internal class SynchronizedReactivePropertyJsonConverter<T> : ReactivePropertyJsonConverter<T>
{
    protected override ReactiveProperty<T> CreateReactiveProperty(T value)
    {
        return new SynchronizedReactiveProperty<T>(value);
    }
}

#endif
