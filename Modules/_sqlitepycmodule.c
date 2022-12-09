#ifndef Py_BUILD_CORE_BUILTIN
#  define Py_BUILD_CORE_MODULE 1
#endif

#define PY_SSIZE_T_CLEAN
#include "Python.h"

#include "sqlite3.h"

static const char* PRAGMA_SQL =
    "PRAGMA page_size=16384;"
    "PRAGMA journal_mode=WAL;"
    "PRAGMA user_version=1;";

static const char* SCHEMA_SQL =
    "CREATE TABLE IF NOT EXISTS pyc ("
    "path TEXT PRIMARY KEY, "
    "data BLOB NOT NULL) "
    "STRICT";

static const char* GET_SQL =
    "SELECT data FROM pyc WHERE path = ?";

static const char* SET_SQL =
    "INSERT OR REPLACE INTO pyc (path, data) VALUES (?, ?)";

typedef struct _sqlitepyc_state {
    sqlite3* db;
    sqlite3_stmt* getStmt;
    sqlite3_stmt* setStmt;
} _sqlitepyc_state;

static inline _sqlitepyc_state*
get_sqlitepyc_state(PyObject* module)
{
    return (_sqlitepyc_state*) PyModule_GetState(module);
}

static PyObject*
_sqlitepyc_init(PyObject* module, PyObject* args)
{
    _sqlitepyc_state* state = get_sqlitepyc_state(module);

    const char* path;

    if (!PyArg_ParseTuple(args, "s", &path))
        return NULL;

    fprintf(stdout, "*** _sqlitepyc.init: %s\n", path);

    int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX;

    // !!! Windows: The encoding used for the path argument must be UTF-8
    int result = sqlite3_open_v2(path, &state->db, flags, NULL);
    if (result != SQLITE_OK) {
        state->db = NULL;
        fprintf(stderr, "*** sqlite3_open_v2 FAILED: %s\n", sqlite3_errstr(result));

        PyErr_SetString(PyExc_RuntimeError, sqlite3_errstr(result));
        return NULL;
    }

    result = sqlite3_exec(state->db, PRAGMA_SQL, NULL, NULL, NULL);
    if (result != SQLITE_OK) {
        state->db = NULL;
        fprintf(stderr, "*** sqlite3_exec FAILED: %s\n", sqlite3_errstr(result));

        PyErr_SetString(PyExc_RuntimeError, sqlite3_errstr(result));
        return NULL;
    }

    result = sqlite3_exec(state->db, SCHEMA_SQL, NULL, NULL, NULL);
    if (result != SQLITE_OK) {
        state->db = NULL;
        fprintf(stderr, "*** sqlite3_exec FAILED: %s\n", sqlite3_errstr(result));

        PyErr_SetString(PyExc_RuntimeError, sqlite3_errstr(result));
        return NULL;
    }

    unsigned int prepareFlags = SQLITE_PREPARE_PERSISTENT;

    result = sqlite3_prepare_v3(state->db, GET_SQL, -1, prepareFlags, &state->getStmt, NULL);
    if (result != SQLITE_OK) {
        state->db = NULL;
        fprintf(stderr, "*** sqlite3_prepare_v3 FAILED: %s\n", sqlite3_errstr(result));

        PyErr_SetString(PyExc_RuntimeError, sqlite3_errstr(result));
        return NULL;
    }

    result = sqlite3_prepare_v3(state->db, SET_SQL, -1, prepareFlags, &state->setStmt, NULL);
    if (result != SQLITE_OK) {
        state->db = NULL;
        fprintf(stderr, "*** sqlite3_prepare_v3 FAILED: %s\n", sqlite3_errstr(result));

        PyErr_SetString(PyExc_RuntimeError, sqlite3_errstr(result));
        return NULL;
    }

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

static int
_sqlitepyc_exec(PyObject* module)
{
    fprintf(stdout, "*** _sqlitepyc.exec\n");

    int result = sqlite3_initialize();
    if (result != SQLITE_OK) {
        PyErr_SetString(PyExc_ImportError, sqlite3_errstr(result));
        return -1;
    }

    return 0;
}

static PyMethodDef _sqlitepyc_methods[] = {
    {"init", _sqlitepyc_init, METH_VARARGS, NULL},
    {"get", _sqlitepyc_get, METH_VARARGS, NULL},
    {"set", _sqlitepyc_set, METH_VARARGS, NULL},
    {NULL, NULL, 0, NULL},
};

static struct PyModuleDef_Slot _sqlitepyc_slots[] = {
    {Py_mod_exec, _sqlitepyc_exec},
    {0, NULL},
};

static void
_sqlitepyc_free(void* module)
{
    _sqlitepyc_state* state = get_sqlitepyc_state(module);

    fprintf(stdout, "*** _sqlitepyc.free\n");

    if (state->getStmt != NULL) {
        int result = sqlite3_finalize(state->getStmt);
        if (result != SQLITE_OK) {
            fprintf(stderr, "*** sqlite3_finalize FAILED: %s\n", sqlite3_errstr(result));
        }
        state->getStmt = NULL;
    };

    if (state->setStmt != NULL) {
        int result = sqlite3_finalize(state->setStmt);
        if (result != SQLITE_OK) {
            fprintf(stderr, "*** sqlite3_finalize FAILED: %s\n", sqlite3_errstr(result));
        }
        state->setStmt = NULL;
    };

    if (state->db != NULL) {
        int result = sqlite3_close_v2(state->db);
        if (result != SQLITE_OK) {
            fprintf(stderr, "*** sqlite3_close_v2 FAILED: %s\n", sqlite3_errstr(result));
        }
        state->db = NULL;
    }

    int result = sqlite3_shutdown();
    if (result != SQLITE_OK) {
        fprintf(stderr, "*** sqlite3_shutdown FAILED: %s\n", sqlite3_errstr(result));
    }

    return;
}

static struct PyModuleDef _sqlitepycmodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_sqlitepyc",
    .m_size = sizeof(_sqlitepyc_state),
    .m_methods = _sqlitepyc_methods,
    .m_slots = _sqlitepyc_slots,
    .m_free = _sqlitepyc_free,
};

PyMODINIT_FUNC
PyInit__sqlitepyc(void)
{
    return PyModuleDef_Init(&_sqlitepycmodule);
}
