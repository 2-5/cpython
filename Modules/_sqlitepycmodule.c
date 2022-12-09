#ifndef Py_BUILD_CORE_BUILTIN
#  define Py_BUILD_CORE_MODULE 1
#endif

#define PY_SSIZE_T_CLEAN
#include "Python.h"

typedef struct _sqlitepyc_state {
    void* db;
} _sqlitepyc_state;

static inline _sqlitepyc_state*
get_sqlitepyc_state(PyObject* module)
{
    return (_sqlitepyc_state*) PyModule_GetState(module);
}

static PyObject*
_sqlitepyc_init(PyObject* module, PyObject* args)
{
    const char* path;

    if (!PyArg_ParseTuple(args, "s", &path))
        return NULL;

    fprintf(stdout, "*** _sqlitepyc.init: %s\n", path);

    Py_RETURN_NONE;
}

static PyObject*
_sqlitepyc_get(PyObject* module, PyObject* args)
{
    const char* path;

    if (!PyArg_ParseTuple(args, "s", &path))
        return NULL;

    fprintf(stdout, "*** _sqlitepyc.get: %s\n", path);

    Py_RETURN_NONE;
}

static PyObject*
_sqlitepyc_set(PyObject* module, PyObject* args)
{
    const char* path;
    Py_buffer buffer;

    if (!PyArg_ParseTuple(args, "sy*", &path, &buffer))
        return NULL;

    fprintf(stdout, "*** _sqlitepyc.set: %s [%lld bytes]\n", path, buffer.len);

    PyBuffer_Release(&buffer);

    Py_RETURN_NONE;
}

static PyMethodDef _sqlitepyc_methods[] = {
    {"init", _sqlitepyc_init, METH_VARARGS, NULL},
    {"get", _sqlitepyc_get, METH_VARARGS, NULL},
    {"set", _sqlitepyc_set, METH_VARARGS, NULL},
    {NULL, NULL, 0, NULL},
};

static void
_sqlitepyc_free(void* module)
{
    _sqlitepyc_state* state = get_sqlitepyc_state(module);

    fprintf(stdout, "*** _sqlitepyc.free\n");

    return;
}

static struct PyModuleDef _sqlitepycmodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_sqlitepyc",
    .m_size = sizeof(_sqlitepyc_state),
    .m_methods = _sqlitepyc_methods,
    .m_free = _sqlitepyc_free,
};

PyMODINIT_FUNC
PyInit__sqlitepyc(void)
{
    return PyModuleDef_Init(&_sqlitepycmodule);
}
