// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.


namespace DotNetty.Common.Concurrency
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

 /**
 * Abstract base class for {@link EventExecutorGroup} implementations that handles their tasks with multiple threads at
 * the same time.
 */

    public abstract class MultithreadEventExecutorGroup : AbstractEventExecutorGroup
    {

        readonly IEventExecutor[] children;
        readonly Set<IEventExecutor> readonlyChildren;
        readonly AtomicInteger childIndex = new AtomicInteger();
        readonly AtomicInteger terminatedChildren = new AtomicInteger();
        readonly Task terminationFuture = Task.FromResult(GlobalEventExecutor.INSTANCE);
        readonly EventExecutorChooser chooser;

        /**
     * @param nEventExecutors           the number of {@link IEventExecutor}s that will be used by this instance.
     *                                  If {@code executorServiceFactory} is {@code null} this number will also be
     *                                  the parallelism requested from the default {@link Executor}. It is generally
     *                                  advised for the number of {@link IEventExecutor}s and the number of
     *                                  {@link Thread}s used by the {@code executorServiceFactory} to lie close
     *                                  together.
     * @param executorServiceFactory    the {@link ExecutorServiceFactory} to use, or {@code null} if the default
     *                                  should be used.
     * @param args                      arguments which will passed to each {@link #newChild(Executor, Object...)} call.
     */

        protected MultithreadEventExecutorGroup(int nEventExecutors, ExecutorServiceFactory executorServiceFactory, params object[] args)
            : this(nEventExecutors, executorServiceFactory != null ? executorServiceFactory.newExecutorService(nEventExecutors) : null, true, args)
        {

        }

        /**
     * @param nEventExecutors   the number of {@link IEventExecutor}s that will be used by this instance.
     *                          If {@code executor} is {@code null} this number will also be the parallelism
     *                          requested from the default {@link Executor}. It is generally advised for the number
     *                          of {@link IEventExecutor}s and the number of {@link Thread}s used by the
     *                          {@code executor} to lie close together.
     * @param executor          the {@link Executor} to use, or {@code null} if the default should be used.
     * @param args              arguments which will passed to each {@link #newChild(Executor, Object...)} call
     */

        protected MultithreadEventExecutorGroup(int nEventExecutors, Executor executor, params object[] args)
            : this(nEventExecutors, executor, false, args)
        {

        }

        MultithreadEventExecutorGroup(int nEventExecutors, Executor executor, bool shutdownExecutor, params object[] args)
        {
            if (nEventExecutors <= 0)
            {
                throw new ArgumentException(String.Format("nEventExecutors: {0} (expected: > 0)", nEventExecutors));
            }

            if (executor == null)
            {
                executor = this.newDefaultExecutorService(nEventExecutors);
                shutdownExecutor = true;
            }

            children = new IEventExecutor[nEventExecutors];
            if (isPowerOfTwo(children.Length))
            {
                chooser = new PowerOfTwoEventExecutorChooser();
            }
            else
            {
                chooser = new GenericEventExecutorChooser();
            }

            for (int i = 0; i < nEventExecutors; i ++)
            {
                bool success = false;
                try
                {
                    children[i] = newChild(executor, args);
                    success = true;
                }
                catch (Exception e)
                {
                    // TODO: Think about if this is a good exception type
                    throw new Exception("failed to create a child event loop", e);
                }
                finally
                {
                    if (!success)
                    {
                        for (int j = 0; j < i; j ++)
                        {
                            children[j].shutdownGracefully();
                        }

                        for (int j = 0; j < i; j ++)
                        {
                            IEventExecutor e = children[j];
                            try
                            {
                                while (!e.isTerminated())
                                {
                                    e.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
                                }
                            }
                            catch (InterruptedException interrupted)
                            {
                                // Let the caller handle the interruption.
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                    }
                }
            }

            readonly bool shutdownExecutor0 = shutdownExecutor;
            readonly Executor executor0 = executor;
            readonly FutureListener<Object> terminationListener = new FutureListener<Object>()
            {
   
            public void operationComplete(Task<Object> future)  {
    if (terminatedChildren.incrementAndGet() == children.length) {
    terminationFuture.setSuccess(null);
    if (shutdownExecutor0) {
    // This cast is correct because shutdownExecutor0 is only try if
    // executor0 is of type ExecutorService.
    ((ExecutorService) executor0).shutdown();
            }
        }
        }
        }
            ;

            foreach (IEventExecutor e in children)
            {
                e.terminationFuture().addListener(terminationListener);
            }

            Set<IEventExecutor> childrenSet = new LinkedHashSet<IEventExecutor>(children.length);
            Collections.addAll(childrenSet, children);
            readonlyChildren = Collections.unmodifiableSet(childrenSet);
        }

        protected ExecutorService newDefaultExecutorService(int nEventExecutors)
        {
            return new DefaultExecutorServiceFactory(getClass()).newExecutorService(nEventExecutors);
        }

        @Override 

        public IEventExecutor next()
        {
            return chooser.next();
        }

        /**
     * Return the number of {@link IEventExecutor} this implementation uses. This number is the maps
     * 1:1 to the threads it use.
     */

        public readonly int executorCount()
        {
            return children.length;
        }


        public override Set<E> children() where E : IEventExecutor
        {
            return (Set<E>)readonlyChildren;
        }

        /**
     * Create a new IEventExecutor which will later then accessible via the {@link #next()}  method. This method will be
     * called for each thread that will serve this {@link MultithreadEventExecutorGroup}.
     *
     */

        protected abstract IEventExecutor newChild(Executor executor, params object[] args);

        public override Task shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit)
        {
            foreach (IEventExecutor l in children)
            {
                l.shutdownGracefully(quietPeriod, timeout, unit);
            }
            return terminationFuture();
        }


        public override Task terminationFuture()
        {
            return terminationFuture;
        }


        [Obsolete]
        public override void shutdown()
        {
            foreach (IEventExecutor l in children)
            {
                l.shutdown();
            }
        }

        public override bool isShuttingDown()
        {
            foreach (IEventExecutor l in children)
            {
                if (!l.isShuttingDown())
                {
                    return false;
                }
            }
            return true;
        }

        public override bool isShutdown()
        {
            foreach (IEventExecutor l in children)
            {
                if (!l.isShutdown())
                {
                    return false;
                }
            }
            return true;
        }

        public override bool isTerminated()
        {
            foreach (IEventExecutor l in children)
            {
                if (!l.isTerminated())
                {
                    return false;
                }
            }
            return true;
        }


        public override bool awaitTermination(long timeout, TimeUnit unit)
        {
            long deadline = System.nanoTime() + unit.toNanos(timeout);
loop:
            foreach (IEventExecutor l in children)
            {
                for (;;)
                {
                    long timeLeft = deadline - System.nanoTime();
                    if (timeLeft <= 0)
                    {
                        goto loop;
                    }
                    if (l.awaitTermination(timeLeft, TimeUnit.NANOSECONDS))
                    {
                        break;
                    }
                }
            }
            return isTerminated();
        }

        static bool isPowerOfTwo(int val)
        {
            return (val & -val) == val;
        }

        interface EventExecutorChooser
        {
            IEventExecutor next();
        }        

        sealed class PowerOfTwoEventExecutorChooser : EventExecutorChooser
        {
            public IEventExecutor next()
            {
                return children[childIndex.getAndIncrement() & children.length - 1];
            }
        }

        class GenericEventExecutorChooser : EventExecutorChooser
        {
            public IEventExecutor next()
            {
                return children[Math.Abs(childIndex.getAndIncrement() % children.length)];
            }
        }
    }