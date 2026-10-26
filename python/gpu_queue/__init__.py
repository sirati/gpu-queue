"""Native GPU admission and cooperative shutdown."""
from ._native import GpuLeaseInterrupted, Lease

__all__ = ['GpuLeaseInterrupted', 'Lease']
