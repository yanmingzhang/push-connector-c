#ifndef DEFS_H
#define DEFS_H

/* This macro looks complicated but it's not: it calculates the address
 * of the embedding struct through the address of the embedded struct.
 * In other words, if struct A embeds struct B, then we can obtain
 * the address of A by taking the address of B and subtracting the
 * field offset of B in A.
 */
#define CONTAINER_OF(ptr, type, field)      \
  ((type *) ((char *) (ptr) - ((char *) &((type *) 0)->field)))

#endif // DEFS_H
