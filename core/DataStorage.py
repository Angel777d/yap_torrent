import logging

import uuid
from typing import List, Dict, Optional, Type, Hashable

logger = logging.getLogger(__name__)


class EntityComponent:
	def __init__(self):
		self.__entity_ref: Optional[Entity] = None

	@classmethod
	def is_hashable(cls) -> bool:
		return False

	def get_hash(self) -> Hashable:
		raise NotImplementedError()

	def _set_entity_ref(self, entity: "Entity") -> None:
		self.__entity_ref = entity

	def _reset(self):
		self.__entity_ref = None

	# mark related entity with marker component
	def add_marker(self, marker_type: Type["EntityComponent"]) -> None:
		self.__entity_ref.add_component(marker_type())

	def _ds(self) -> "DataStorage":
		self.__entity_ref._get_ds()


class Entity:
	def __init__(self, ds: "DataStorage"):
		self.__ds = ds
		self.__entity_id: int = 0
		self.__components: Dict[Type[EntityComponent], EntityComponent] = {}

	@property
	def entity_id(self) -> int:
		return self.__entity_id

	def has_component[T:EntityComponent](self, component_type: Type[T]) -> bool:
		return component_type in self.__components

	def add_component(self, component: EntityComponent) -> "Entity":
		if type(component) in self.__components:
			logger.warning(f"Component {type(component)} already added")

			return self
		self.__components[type(component)] = component
		component._set_entity_ref(self)
		self.__ds._add_component(self, component)
		return self

	def remove_component[T:EntityComponent](self, component_type: Type[T]) -> "Entity":
		self.__ds._remove_component(self, component_type)
		self.get_component(component_type)._reset()
		del self.__components[component_type]
		return self

	def get_component[T:EntityComponent](self, component_type: Type[T]) -> Optional[T]:
		return self.__components.get(component_type)

	def is_valid(self) -> bool:
		return bool(self.__entity_id)

	def _init(self, entity_id: int) -> "Entity":
		self.__entity_id = entity_id
		return self

	def _reset(self) -> int:
		for c_type, component in self.__components.items():
			self.__ds._remove_component(self, c_type)
			component._reset()
		self.__components.clear()
		result = self.__entity_id
		self.__entity_id = 0
		return result

	def __repr__(self):
		return f"Entity {self.__entity_id} ({', '.join(c.__name__ for c in self.__components.keys())})"

	def _get_ds(self) -> "DataStorage":
		return self.__ds


class _Collection:
	@property
	def entities(self) -> List[Entity]:
		raise NotImplementedError()

	def find(self, search_value: Hashable) -> Optional[Entity]:
		raise RuntimeError("Not hashable collection can't use find")

	def _add(self, entity: Entity, component: EntityComponent) -> None:
		raise NotImplementedError()

	def _remove(self, entity: Entity, component_type: type) -> None:
		raise NotImplementedError()

	def __len__(self):
		raise NotImplementedError()


class HashCollection(_Collection):
	def __init__(self):
		self.__data: Dict[Hashable, Entity] = {}

	@property
	def entities(self) -> List[Entity]:
		return list(self.__data.values())

	def find(self, search_value: Hashable) -> Optional[Entity]:
		return self.__data.get(search_value, None)

	def _add(self, entity: Entity, component: EntityComponent) -> None:
		self.__data[component.get_hash()] = entity

	def _remove[T: EntityComponent](self, entity: Entity, component_type: Type[T]) -> None:
		del self.__data[entity.get_component(component_type).get_hash()]

	def __len__(self):
		return len(self.__data)


class ListCollection(_Collection):
	def __init__(self):
		self.__data: List[Entity] = []

	@property
	def entities(self) -> List[Entity]:
		return self.__data.copy()

	def _add(self, entity: Entity, component: EntityComponent) -> None:
		self.__data.append(entity)

	def _remove(self, entity: Entity, _: type) -> None:
		self.__data.remove(entity)

	def __len__(self):
		return len(self.__data)


class DataStorage:
	def __init__(self):
		self.__entities: Dict[int, Entity] = {}
		self.__collections: Dict[type, _Collection] = {}

	def create_entity(self) -> Entity:
		eid = uuid.uuid4().int
		entity = Entity(self)
		self.__entities[eid] = entity._init(eid)
		return entity

	def remove_entity(self, entity: Entity) -> None:
		del self.__entities[entity._reset()]

	def get_entity(self, eid: int) -> Optional[Entity]:
		return self.__entities.get(eid, None)

	def get_collection[T: EntityComponent](self, component_type: Type[T]) -> _Collection:
		return self.__collections.setdefault(
			component_type,
			HashCollection() if component_type.is_hashable() else ListCollection()
		)

	#
	def clear_collection[T: EntityComponent](self, component_type: Type[T]) -> None:
		entities = self.get_collection(component_type).entities
		for entity in entities:
			self.remove_entity(entity)

	def erase_collection[T: EntityComponent](self, component_type: Type[T]) -> None:
		entities = self.__collections.get(component_type).entities
		for entity in entities:
			entity.remove_component(component_type)

	def _add_component(self, entity: Entity, component: EntityComponent) -> None:
		self.__collections.setdefault(
			type(component),
			HashCollection() if component.is_hashable() else ListCollection()
		)._add(entity, component)

	def _remove_component[T: EntityComponent](self, entity: Entity, component_type: Type[T]) -> None:
		self.__collections.get(component_type)._remove(entity, component_type)
