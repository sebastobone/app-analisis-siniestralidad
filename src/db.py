from typing import TypeVar

from sqlmodel import SQLModel, select

from src.dependencias import SessionDep

T = TypeVar("T", bound=SQLModel)
C = TypeVar("C")


def obtener_fila[T, C](
    session: SessionDep, tabla: type[T], columna: C, valor: C
) -> T | None:
    return session.exec(select(tabla).where(columna == valor)).one_or_none()


def guardar_fila[T](session: SessionDep, fila: T) -> None:
    session.add(fila)
    session.commit()
    session.refresh(fila)


def eliminar_fila[T, C](
    session: SessionDep, tabla: type[T], columna: C, valor: C
) -> None:
    fila = obtener_fila(session, tabla, columna, valor)
    if fila:
        session.delete(fila)
        session.commit()
